package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import (
	"sync/atomic"
	"sort"
)

type ServerPing struct {
	server string
	viewNum uint
	pingTime time.Time
	eMsg string
}

func (sp *ServerPing) Error() string  {
	return sp.eMsg
}

type ServerPingList []ServerPing

func (spl ServerPingList) Len() int { return len(spl) }
func (spl ServerPingList) Less(i, j int) bool { return spl[i].pingTime.Before(spl[j].pingTime) }
func (spl ServerPingList) Swap(i, j int){ spl[i], spl[j] = spl[j], spl[i] }

func (vs* ViewServer) getLatest(except *map[string]struct {}) (ServerPing, error) {
	if len(vs.pings) <= 0 {
		return ServerPing{}, &ServerPing{"", 1, time.Time{}, "no servers available1"}
	}
	spl := make(ServerPingList, len(vs.pings))
	i := 0
	for _,v := range vs.pings {
		if _,ok := (*except)[v.server]; !ok {
			spl[i] = v
			i++;
		}
	}
	sort.Sort(sort.Reverse(spl))
	if vs.checkDead(spl[0].server) {
		return ServerPing{}, &ServerPing{"", 1, time.Time{}, "no servers available2"}
	} else {
		return spl[0], nil
	}
}

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string
	view	 VSView
	pings map[string]ServerPing
	// Your declarations here.
}

/**
*	returns alive server with latest ping
*/
func (vs *ViewServer) getView(server string, viewNum uint) VSView {
	vs.mu.Lock()
	currentView := vs.view
	if viewNum == 0 {
		if server == currentView.view.Primary {
			currentView.view.Primary = currentView.view.Backup
			currentView.view.Backup = ""
			currentView.view.Viewnum += 1
			currentView.pendingAck = true
		} else if server == currentView.view.Backup {
			currentView.view.Backup = ""
		}
	}
	if currentView.pendingAck {
		if server == currentView.view.Primary {
			if currentView.view.Viewnum == viewNum {
				currentView.pendingAck = false
				except := make(map[string]struct {}, 2)
				except[vs.view.view.Primary] = struct {}{}
				except[vs.view.view.Backup] = struct {}{}
				backupServer, error := vs.getLatest(&except)
				if error == nil {
					currentView.view.Backup = backupServer.server
					currentView.view.Viewnum += 1
					currentView.pendingAck = true
				} else {
					fmt.Errorf("Error: ", error)
					//log.Println("Error: ", error)
				}
			}
		}
	} else {
		except := make(map[string]struct {}, 2)
		except[vs.view.view.Primary] = struct {}{}
		except[vs.view.view.Backup] = struct {}{}
		if currentView.view.Primary == "" {
			latestServer, error := vs.getLatest(&except)
			if error == nil {
				currentView.view.Primary = latestServer.server
				currentView.view.Viewnum += 1
				currentView.pendingAck = true
			} else {
				fmt.Errorf("Error: ", error)
				//log.Println("Error: ", error)
			}
		} else if currentView.view.Backup == "" {
			backupServer, error := vs.getLatest(&except)
			if error == nil {
				currentView.view.Backup = backupServer.server
				currentView.view.Viewnum += 1
				currentView.pendingAck = true
			} else {
				fmt.Errorf("Error: ", error)
				//log.Println("Error: ", error)
			}
		}
	}
//	if currentView.view.Primary == "" && currentView.view.Backup == "" {
//		vs.Kill()
//		os.Exit(1)
//	} else {
//		vs.view = currentView
//	}
	vs.view = currentView
	vs.mu.Unlock()
	return currentView
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	//log.Println("begin ping: ", vs.view.pendingAck, " ", vs.view.view)
//	log.Print("server: ", args.Me, " ", args.Viewnum)
	serverPing := ServerPing{args.Me, args.Viewnum, time.Now(), ""}
	vs.pings[args.Me] = serverPing
	vsView := vs.getView(args.Me, args.Viewnum)
	//log.Println("end ping: ", vs.view.pendingAck, " ", vs.view.view)
	reply.View = *vsView.view
	// Your code here.
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	reply.View = *vs.view.view
	// Your code here.
	return nil
}

func (vs *ViewServer) checkDead(server string) bool {
	lastPing, ok := vs.pings[server]
	if server == "" || !ok {
		return true
	}
	timeNow := time.Now()
	if timeNow.Sub(lastPing.pingTime) / PingInterval >= DeadPings {
		return true
	} else {
		return false
	}
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
	vs.mu.Lock()
	currentView := vs.view
	if currentView.view.Primary != "" && !currentView.pendingAck {
		primaryDead := vs.checkDead(currentView.view.Primary)
		if primaryDead {
			if currentView.view.Backup != "" {
				currentView.view.Primary = currentView.view.Backup
				currentView.view.Viewnum += 1
				currentView.view.Backup = ""
				currentView.pendingAck = true
			}
		}
	}

	if currentView.view.Backup != "" {
		backupDead := vs.checkDead(currentView.view.Backup)
		if backupDead {
			except := make(map[string]struct {}, 2)
			except[vs.view.view.Primary] = struct {}{}
			except[vs.view.view.Backup] = struct {}{}
			latest, error := vs.getLatest(&except)
			if error == nil {
				currentView.view.Backup = latest.server
				currentView.view.Viewnum += 1
				currentView.pendingAck = true
			}
		}
	}
	vs.view = currentView
	vs.mu.Unlock()
	// Your code here.
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.pings = make(map[string]ServerPing)
	vs.view = VSView{&View{0, "", ""}, false}
	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
