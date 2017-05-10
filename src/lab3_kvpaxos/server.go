package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import (
	"math/rand"
	"time"
	"reflect"
	"strconv"
)


const Debug = 0
const MaxUint = ^uint(0)
const MaxInt = int(MaxUint >> 1)
type Fate int

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op string
	Key string
	Value string
	Uid int64
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos
	stateMachine	map[string]string
	instanceProcessed int
	requestHistory map[int64]int
	logUpdateMutex	sync.Mutex
	// Your definitions here.
}

func (kv *KVPaxos) updateStateMachine() {
	sleepTime := 10 * time.Millisecond
	for !kv.isdead() {
		//fmt.Println("in instanceProcessed: ", kv.instanceProcessed, " max: ", kv.px.Max(), " min: ", kv.px.Min(), " server: ", kv.me)
		if kv.px.Max() > kv.instanceProcessed {
			for i:= kv.instanceProcessed+1; i<=kv.px.Max(); i++ {
				fate, value := kv.px.Status(i)
				//fmt.Println("updatemachine seq: ", i, " fate: ", fate, " val: ", value, " server: ", kv.me)
				if fate == paxos.Decided {
					val := value.(Op)
					_, present := kv.requestHistory[val.Uid]
					if !present {
						if val.Op == PUT {
							//fmt.Println("putting key: ", val.Key, " value: ", val.Value, " server: ", kv.me)
							kv.mu.Lock()
							kv.stateMachine[val.Key] = val.Value
							kv.mu.Unlock()
						} else if val.Op == APPEND {
							//fmt.Println("appending key: ", val.Key, " value: ", val.Value, " server: ", kv.me)
							kv.mu.Lock()
							prevVal := ""
							currVal, present := kv.stateMachine[val.Key]
							if present {
								prevVal = currVal
							}
							kv.stateMachine[val.Key] = prevVal + val.Value
							kv.mu.Unlock()
						}
						kv.requestHistory[val.Uid] = 0
					}
					kv.px.Done(i)
					kv.instanceProcessed = i
				} else if fate == paxos.Forgotten {
					kv.updateLogs(i)
					i--
				} else {
					break
				}
			}
		}
		//fmt.Println("after update state machine statemachine: ", kv.stateMachine, " server: ", kv.me)
		time.Sleep(sleepTime)
	}
}

func (kv *KVPaxos) updateLogs(seq int) {
	kv.logUpdateMutex.Lock()
	//fmt.Println("in updatelog with seq: ", seq, " server: ", kv.me)
	getVal := rand.Intn(MaxInt)
	val := Op{GET, "", strconv.Itoa(getVal), 0}
	to := 10 * time.Millisecond
	kv.px.Start(seq, val)
	for !kv.isdead() {
		fate, value := kv.px.Status(seq)
		if fate == paxos.Decided || fate == paxos.Forgotten {
			if reflect.DeepEqual(val, value) {
				//fmt.Println("added get val in log for updatelogs: ", seq, " server: ", kv.me)
				break
			} else {
				//fmt.Println("didn't add get val in log for updatelogs: ", seq, " server: ", kv.me)
				seq = seq + 1
				kv.px.Start(seq, val)
			}
		}
		time.Sleep(to)
//		if to < 10 * time.Second {
//			to *= 2
//		}
	}
	kv.logUpdateMutex.Unlock()
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	kv.logUpdateMutex.Lock()
	//fmt.Println("in get: ", args, " server: ", kv.me)
//	kv.mu.Lock()
//	_, exists := kv.stateMachine[args.Key]
//	kv.mu.Unlock()

	getVal := rand.Intn(MaxInt)
	val := Op{GET, args.Key, strconv.Itoa(getVal), args.UID}
	seq := kv.instanceProcessed + 1
	to := 10 * time.Millisecond
	kv.px.Start(seq, val)
	for !kv.isdead() {
		fate, value := kv.px.Status(seq)
		//fmt.Println("get fate: ", fate, " val: ", value, " seq: ", seq, " server: ", kv.me)
		if fate == paxos.Decided || fate == paxos.Forgotten {
			if reflect.DeepEqual(val, value) {
				//fmt.Println("in get added get val in log: ", args, " seq: ", seq, " server: ", kv.me)
				break
			} else {
				//fmt.Println("in get didn't add get val in log: ", args, " seq: ", seq," server: ", kv.me)
				if kv.instanceProcessed > seq {
					seq = kv.instanceProcessed + 1
				} else {
					seq = seq + 1
				}
				kv.px.Start(seq, val)
			}
		}
		time.Sleep(to)
//		if to < 10 * time.Second {
//			to *= 2
//		}
	}
	kv.logUpdateMutex.Unlock()
	for kv.instanceProcessed < kv.px.Max() && !kv.isdead() {
		//fmt.Println("min: ", kv.instanceProcessed, " max: ", kv.px.Max(), " server: ", kv.me)
		time.Sleep(10 * time.Millisecond)
	}
	//fmt.Println("statemachine: ", kv.stateMachine, " server: ", kv.me)
	kv.mu.Lock()
	keyVal, exists := kv.stateMachine[args.Key]
	kv.mu.Unlock()
	if !exists {
		//fmt.Println("key: ", args.Key, " doesn't exist", " server: ", kv.me)
		reply.Err = ErrNoKey
		return nil
	} else {
		reply.Err = ""
		reply.Value = keyVal
	}

	// Your code here.
	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	//fmt.Println("in putappend: ", args, " server: ", kv.me, " server: ", kv.me)
	// Your code here.
	val := Op{args.Op, args.Key, args.Value, args.UID}
	seq := kv.px.Max() + 1
	kv.px.Start(seq, val)
	to := 20 * time.Millisecond
	for !kv.isdead() {
		fate, value := kv.px.Status(seq)
		if fate == paxos.Decided || fate == paxos.Forgotten {
			if reflect.DeepEqual(val, value) {
				//fmt.Println("in putappend added value to log: ", args, " seq: ", seq, " server: ", kv.me)
				break
			} else {
				seq = kv.px.Max() + 1
				kv.px.Start(seq, val)
			}
		}
		time.Sleep(to)
//		if to < 10 * time.Second {
//			to *= 1.5
//		}
	}
	reply.Err = ""
	return nil
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me
	kv.instanceProcessed = -1
	kv.requestHistory = make(map[int64]int)
	// Your initialization code here.

	rpcs := rpc.NewServer()
	rpcs.Register(kv)
	kv.stateMachine = make(map[string]string)
	kv.px = paxos.Make(servers, me, rpcs)
	go kv.updateStateMachine()
	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l


	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && kv.isdead() == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
