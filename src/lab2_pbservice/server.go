package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync/atomic"
import "os"
import "syscall"
import (
	"math/rand"
	"sync"
)

const (
	EXTRA = "extra"
	PRIMARY = "primary"
	BACKUP = "backup"
	CACHE_LIFE = time.Millisecond * 5 * 60 * 1000
)

type PBServer struct {
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	mu         sync.Mutex
	db         map[string]string
	view       *viewservice.View
	getRequestCache	map[int64]GetReply
	putAppendRequestCache	map[int64]PutAppendReply
	DBServer
	currentStatus	string
	extraDBServer	*ExtraDBServer
	primaryDBServer *PrimaryDBServer
	backupDBServer	*BackupDBServer
	backupInSync	bool
	// Your declarations here.
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	log.Println("me: ", pb.me)
	log.Println("got get request: ", args)
	return pb.DBGet(args, reply, pb)
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
//	log.Println("number: ", pb.me)
	log.Println("got get request: ", args)
	return pb.DBPutAppend(args, reply, pb)
}

func (pb *PBServer) CopyDBPBServer(args *CopyDBArgs, reply *CopyDBReply) error {
	return pb.CopyDB(args, reply, pb)
}

func (pb *PBServer) BackupGetPB(args *GetArgs, reply *GetReply) error {
	return pb.BackupGet(args, reply, pb)
}

func (pb *PBServer) BackupPutAppendPB(args *PutAppendArgs, reply *PutAppendReply) error {
	return pb.BackupPutAppend(args, reply, pb)
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {
	updateView(pb)
	pb.updateCache()
	// Your code here.
}

func (pb *PBServer) updateCache()  {
	now := time.Now()
	tempPutRequestCache := pb.putAppendRequestCache
	for k, _ := range tempPutRequestCache {
		if now.Sub(pb.putAppendRequestCache[k].Time) > CACHE_LIFE {
			delete(pb.putAppendRequestCache, k)
		}
	}
	tempGetRequestCache := pb.getRequestCache
	for k, _ := range tempGetRequestCache {
		if now.Sub(pb.getRequestCache[k].Time) > CACHE_LIFE {
			delete(pb.getRequestCache, k)
		}
	}
}

func (pb *PBServer) switchToPrimary(pastView *viewservice.View) {
	switch pb.currentStatus {
	case EXTRA:
		log.Println("Switching to Primary from extra server")
	case BACKUP:
		log.Println("Switching to Primary from Backup")
		pb.backupInSync = false
		if pb.view.Backup != "" {
			go func() {
				pb.sendDataToBackup()
			}()
		}
	case PRIMARY:
		if pb.view.Backup != "" && !pb.backupInSync {
			log.Println("got new backup, sending db data")
			go func() {
				pb.sendDataToBackup()
			}()
		}
		if pastView.Backup != "" && pb.view.Backup != pastView.Backup {
			log.Println("got backup different from old one, sending db data")
			pb.backupInSync = false
			go func() {
				pb.sendDataToBackup()
			}()
		}
	}
	pb.currentStatus = PRIMARY
	pb.DBServer = *pb.primaryDBServer
}

func (pb *PBServer) switchToBackup(pastView *viewservice.View) {
	log.Println("in switch to backup")
	switch pb.currentStatus {
	case EXTRA:
		log.Println("Switching to Backup from Extra")
		pb.db = make(map[string]string)
		pb.backupInSync = false
	case PRIMARY:
		log.Println("Switching to Backup from Primary")
		pb.db =  make(map[string]string)
		pb.getRequestCache = make(map[int64]GetReply)
		pb.putAppendRequestCache = make(map[int64]PutAppendReply)
		pb.backupInSync = false
	}
	pb.currentStatus = BACKUP
	pb.DBServer = *pb.backupDBServer
}

func (pb *PBServer) switchToExtra(pastView *viewservice.View) {
	switch pb.currentStatus {
	case BACKUP:
		log.Println("Switching to Etra from Backup")
		pb.db = make(map[string]string)
		pb.backupInSync = false
		pb.getRequestCache = make(map[int64]GetReply)
		pb.putAppendRequestCache = make(map[int64]PutAppendReply)
	case PRIMARY:
		log.Println("Switching to Extra from Primary")
		pb.db = make(map[string]string)
		pb.backupInSync = false
		pb.getRequestCache = make(map[int64]GetReply)
		pb.putAppendRequestCache = make(map[int64]PutAppendReply)
	}
	pb.currentStatus = EXTRA
	pb.DBServer = *pb.extraDBServer
}

func (pb *PBServer) sendDataToBackup() {
	args := &CopyDBArgs{pb.db}
	reply := CopyDBReply{}
	for {
		ok := call(pb.view.Backup, "PBServer.CopyDBPBServer", args, &reply)
		if ok {
			pb.backupInSync = true
			break
		}
	}
	if reply.Err == ErrWrongServer {
		updateView(pb)
		if pb.view.Primary == pb.me {
			pb.sendDataToBackup()
		}
	}
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.DBServer = ExtraDBServer{}
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	pb.db = make(map[string]string)
	pb.getRequestCache = make(map[int64]GetReply)
	pb.putAppendRequestCache = make(map[int64]PutAppendReply)
	pb.view = &viewservice.View{0, "", ""}
	pb.currentStatus = EXTRA
	pb.backupDBServer = &BackupDBServer{}
	pb.extraDBServer = &ExtraDBServer{}
	pb.primaryDBServer = &PrimaryDBServer{}
	pb.backupInSync = false

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
