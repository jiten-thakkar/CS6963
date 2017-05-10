package pbservice

import "viewservice"
import "net/rpc"
import "fmt"

import "crypto/rand"
import (
	"math/big"
	"log"
	"time"
)


type Clerk struct {
	vs *viewservice.Clerk
	me     string // client's name (host:port)
	vshost string
	view 	*viewservice.View
	haveView	bool
}

// this may come in handy.
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(vshost string, me string) *Clerk {
	ck := new(Clerk)
	ck.vs = viewservice.MakeClerk(me, vshost)
	ck.me = me
	ck.vshost = vshost
	v, ok := ck.vs.Get()
	ck.haveView = ok
	ck.view = &viewservice.View{}
	if ok {
		*ck.view = v
	}
	// Your ck.* initializations here

	return ck
}


//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will return an
// error after a while if the server is dead.
// don't provide your own time-out mechanism.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func (ck *Clerk) updateView()  {
	log.Println("in client update view")
	for {
		v, ok := ck.vs.Get()
		if ok {
			*ck.view = v
			ck.haveView = true
			break
		}
	}
}

func (ck *Clerk) getPrimary() string {
	if ck.haveView {
		return ck.view.Primary
	} else {
		ck.updateView()
		return ck.view.Primary
	}
}

//
// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
//
func (ck *Clerk) Get(key string) string {
//	uID := ck.me + strconv.FormatInt(nrand(), 10)
	uID := nrand()
	if ck.view.Primary == "" {
		v, ok := ck.vs.Get()
		ck.haveView = ok
		ck.view = &viewservice.View{}
		if ok {
			*ck.view = v
		}
		log.Println("got v as: ", v)
	}
	primary := ck.view.Primary
	args := GetArgs{key, uID}
	var reply GetReply
	for {
		ok := call(primary, "PBServer.Get", &args, &reply)
		if ok {
			break
		} else {
			time.Sleep(viewservice.PingInterval)
			log.Println("before primary: ", ck.view.Primary)
			ck.updateView()
			primary = ck.view.Primary
			log.Println("after primary: ", ck.view.Primary)
		}
	}
	// Your code here.
	if reply.Err == OK {
		return reply.Value
	} else if reply.Err == ErrWrongServer {
		ck.updateView()
		return ck.Get(key)
	} else if reply.Err == BackupNotReady {
		return ck.Get(key)
	} else if reply.Err == ErrNoKey {
		return "NOKEY"
	} else {
		log.Println("unknown err: ", reply.Err)
		ck.updateView()
		return ck.Get(key)
	}
}

//
// send a Put or Append RPC
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	uID := nrand()
	if ck.view.Primary == "" {
		v, ok := ck.vs.Get()
		ck.haveView = ok
		ck.view = &viewservice.View{}
		if ok {
			*ck.view = v
		}
		log.Println("got v as: ", v)
	}
	primary := ck.view.Primary
	args := &PutAppendArgs{key, value, op, uID}
	var reply PutAppendReply
	for {
		ok := call(primary, "PBServer.PutAppend", args, &reply)
		if ok {
			break
		} else {
			time.Sleep(viewservice.PingInterval)
			log.Println("before primary: ", ck.view.Primary)
			ck.updateView()
			primary = ck.view.Primary
			log.Println("after primary: ", ck.view.Primary)
		}
	}
	if reply.Err == ErrWrongServer {
		ck.updateView()
		ck.PutAppend(key, value, op)
	} else if reply.Err == BackupNotReady {
		time.Sleep(viewservice.PingInterval)
		ck.PutAppend(key, value, op)
	} else if reply.Err == OK {
		return
	} else {
		log.Println("unknown err: ", reply.Err)
		ck.updateView()
		ck.PutAppend(key, value, op)
	}
}

//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

//
// tell the primary to append to key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
