package kvpaxos

import "net/rpc"
import "crypto/rand"
import "math/big"

import (
	"fmt"
	"time"
)

type Clerk struct {
	servers []string
	ranNumber int64
	totalServers int
	lastSuccessful int64
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []string) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.ranNumber = nrand()
	ck.totalServers = len(servers)
	ck.lastSuccessful = -1
	// You'll have to add code here.
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

func (ck *Clerk) getUID() int64 {
	prevRandNumber := ck.ranNumber
	ck.ranNumber = prevRandNumber + 1
	return prevRandNumber + 1
}

func (ck *Clerk) getNextServer(num int) (string, int) {
	newServer := (num+1)%ck.totalServers
	return ck.servers[newServer], newServer
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
	uId := nrand()
	request := &GetArgs{key, uId, ck.lastSuccessful}
	reply := &GetReply{}
	serverNum := 0
	server := ck.servers[serverNum]
	to := 10 * time.Millisecond
	for {
		Ok := call(server, "KVPaxos.Get", request, reply)
		if Ok {
			break
		}
		server, serverNum = ck.getNextServer(serverNum)
		time.Sleep(to)
//		if to < 10 * time.Second {
//			to *= 2
//		}
	}
	// You will have to modify this function.
	ck.lastSuccessful = uId
	if reply.Err == ErrNoKey {
		return ""
	}
	return reply.Value
}

//
// shared by Put and Append.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	uId := nrand()
	request := &PutAppendArgs{key, value, op, uId, ck.lastSuccessful}
	reply := &PutAppendReply{}
	serverNum := 0
	server := ck.servers[serverNum]
	to := 10 * time.Millisecond
	for {
		Ok := call(server, "KVPaxos.PutAppend", request, reply)
		if Ok {
			if reply.Err == "" {
				break
			}
		}
		server, serverNum = ck.getNextServer(serverNum)
		time.Sleep(to)
//		if to < 10 * time.Second {
//			to *= 2
//		}
	}
	// You will have to modify this function.
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PUT)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, APPEND)
}
