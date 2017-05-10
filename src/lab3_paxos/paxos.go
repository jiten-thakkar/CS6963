package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"

import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import (
	"math/rand"
	"time"
	"runtime"
)


// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten    // decided but forgotten.
)

const MaxUint = ^uint(0)
const MaxInt = int(MaxUint >> 1)

type Paxos struct {
	mu         sync.Mutex
	muPeerInstances sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	totalPeers	int
	me         int // index into peers[]
	peerInstances	map[string]int
	history		*History
	n_p int
	n_a int
	v_a interface{}
	highestNumber int
	//fate Fate	//latest fate
	// Your data here.
}

type History struct {
	instances map[int]Instance
	oldest int
	latest int
	z_i int
	minimumOfAll int
	mu sync.Mutex
	mu1 sync.Mutex
}

func (his *History) add(instance Instance) {
	his.mu1.Lock()
	his.instances[instance.sequence] = instance
	his.mu1.Unlock()
	if instance.sequence < his.oldest {
		his.oldest = instance.sequence
	}
	if instance.sequence > his.latest && instance.fate == Decided {
		his.latest = instance.sequence
	}
}

func (his *History) forgetBelow(seq int) {
	if seq > his.oldest {
		toDelete := []int{}
		smallest := MaxInt
		his.mu.Lock()
		for k, _ := range his.instances {
			if k <= seq {
				toDelete = append(toDelete, k)
			} else {
				if k < smallest {
					smallest = k
				}
			}
		}
		for k := range toDelete {
			delete(his.instances, k)
		}
		his.oldest = smallest
		his.mu.Unlock()
		runtime.GC()
	}
}

type Instance struct {
	sequence int
	value interface{}
	fate Fate
	n	int
}

type PrepareRequest struct {
	Number int
	MaxFreeable int
	Peer string
	Seq int
}

type PrepareReply struct {
	Ok bool
	Number int
	Number_Accepted int
	Value_Accepted interface{}
	HighestNumber int
	MaxFreeable int
	Seq_Decided bool
	Seq_val interface{}
	Seq_num	int
}

type AcceptRequest struct {
	Number int
	Value interface{}
	MaxFreeable int
	Peer string
	Seq int
}

type AcceptReply struct {
	Ok bool
	Sequence int
	MaxFreeable int
	HighestNumber int
	Seq_Decided bool
	Seq_val interface{}
	Seq_num	int
}

type DecideRequest struct {
	Sequence int
	Value interface{}
	Number int
	MaxFreeable int
	Peer string
}

type DecideReply struct {
	Ok bool
	MaxFreeable int
	HighestNumber int
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	//fmt.Println(err)
	return false
}


//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	val, present := px.history.instances[seq]
	if (present && val.fate == Decided) || seq <= px.history.minimumOfAll {
		return
	}
	instance := Instance{}
	instance.value = v
	instance.fate = Pending
	instance.sequence = seq
	px.history.add(instance)
	go func() {
		px.propose(seq, v)
	}()
}


func (px *Paxos) triggerDone() {
	//fmt.Println("in trigger modone ", px.peers[px.me])
	if len(px.peerInstances) == px.totalPeers {
		minimum := MaxInt
		for _, v := range px.peerInstances {
			//fmt.Println("peer: ", k, " value: ", v, " ", px.peers[px.me])
			if v < minimum {
				minimum = v
			}
		}
		//fmt.Println("minimum: ", minimum, " ", px.peers[px.me])
		if minimum != MaxInt && minimum != -1 {
			//fmt.Println("server: ", px.peers[px.me], " minimum decided: ", minimum)
			//fmt.Println("server: ", px.peers[px.me], " before instances: ", px.history.instances)
			px.history.forgetBelow(minimum)
			//fmt.Println("server: ", px.peers[px.me], " after instances: ", px.history.instances)
		}
		px.history.minimumOfAll = minimum
	}
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	//fmt.Println("server: ", px.me, "received done seq: ", seq, " current z_i: ", px.history.z_i)
	// Your code here.
	if seq > px.history.z_i {
		px.history.z_i = seq
	}
	//px.history.forgetBelow(seq)
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	return px.history.latest
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	// You code here.
	return px.history.minimumOfAll + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	instance, present := px.history.instances[seq]
	if present {
		return instance.fate, instance.value
	} else {
		return Forgotten, nil
	}
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

func (px *Paxos) propose(proposedSequence int, v interface{}) {
	for {
		val := px.history.instances[proposedSequence]
		if val.fate == Decided {
			break
		}
		//fmt.Println("server: ", px.peers[px.me], " instances: ", px.history.instances)
		number := px.highestNumber + 1
		px.mu.Lock()
		px.highestNumber = number
		px.mu.Unlock()
		prepareReplies := make(chan PrepareReply, px.totalPeers)
		requestOwn := &PrepareRequest{number, px.history.z_i, px.peers[px.me], proposedSequence}
		replyOwn := &PrepareReply{}
		px.Prepare(requestOwn, replyOwn)
		px.peerInstances[px.peers[px.me]] = replyOwn.MaxFreeable
		prepareReplies <- *replyOwn
		for _, peer := range px.peers {
			if peer != px.peers[px.me] {
				peerName := peer
				go func() {
					request := &PrepareRequest{number, px.history.z_i, px.peers[px.me], proposedSequence}
					reply := &PrepareReply{}
					Ok := call(peerName, "Paxos.Prepare", request, reply)
					if Ok {
						prepareReplies <- *reply
						px.peerInstances[peer] = reply.MaxFreeable
					} else {
						reply.Ok = false
						prepareReplies <- *reply
					}
				}()
			}
		}
		totalOk := 0
		maxN_A := px.n_a
		valOfMaxN_A := v
		i := 0
		seqAlreadyDone := false
		for v := range prepareReplies {
			//fmt.Println("got reply: ", v)
			i += 1
//			//fmt.Println("in for loop for: ", px.peers[px.me], " with i: ", i)
			if v.Seq_Decided {
//				px.history.add(Instance{proposedSequence, v.Seq_val, Decided, v.Seq_num})
				px.sendDecide(proposedSequence, v.Seq_num, v.Seq_val)
				seqAlreadyDone = true
				break
			}
			if v.Ok {
				totalOk += 1
				//fmt.Println("for server: ", px.peers[px.me], " reply: ", v)
				if v.Number_Accepted > maxN_A {
					maxN_A = v.Number_Accepted
					valOfMaxN_A = v.Value_Accepted
				}
			}
			if i == px.totalPeers {
				close(prepareReplies)
			}
			px.mu.Lock()
			if v.HighestNumber > px.highestNumber {
				px.highestNumber = v.HighestNumber
			}
			px.mu.Unlock()
		}
		if seqAlreadyDone {
			break
		}
//		//fmt.Println("here")
		if totalOk >= (px.totalPeers / 2) + 1 {
//			fmt.Print("got majority in: ", px.peers[px.me])
			accepted := px.sendAccept(proposedSequence, number, valOfMaxN_A)
			if accepted {
				break
			}
		} else {
//			fmt.Print("failed to get majority in: ", px.peers[px.me])
		}
		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	}
	go func() {
		px.triggerDone()
	}()
}

func (px *Paxos) sendAccept(proposedSequence int, number int, v interface{}) bool {
	val := px.history.instances[proposedSequence]
	if val.fate == Decided {
		return true
	}
	acceptReplies := make(chan AcceptReply, px.totalPeers)
	requestOwn := &AcceptRequest{number, v, px.history.z_i, px.peers[px.me], proposedSequence}
	replyOwn := &AcceptReply{}
	px.Accept(requestOwn, replyOwn)
	px.peerInstances[px.peers[px.me]] = replyOwn.MaxFreeable
	acceptReplies <- *replyOwn
	for _, peer := range px.peers {
		if peer != px.peers[px.me] {
			peerName := peer
			go func() {
				request := &AcceptRequest{number, v, px.history.z_i, px.peers[px.me], proposedSequence}
				reply := &AcceptReply{}
				Ok := call(peerName, "Paxos.Accept", request, reply)
				if Ok {
					acceptReplies <- *reply
					px.peerInstances[peer] = reply.MaxFreeable
				} else {
					reply.Ok = false
					acceptReplies <- *reply
				}
			}()
		}
	}
	totalOk := 0
	i := 0
	for v := range acceptReplies {
		if v.Seq_Decided {
//			px.history.add(Instance{proposedSequence, v.Seq_val, Decided, v.Seq_num})
			px.sendDecide(proposedSequence, v.Seq_num, v.Seq_val)
			return true
		}
		if v.Ok {
			totalOk += 1
		}
		i += 1
		if i == px.totalPeers {
			close(acceptReplies)
		}
		px.mu.Lock()
		if v.HighestNumber > px.highestNumber {
			px.highestNumber = v.HighestNumber
		}
		px.mu.Unlock()
	}
	if totalOk >= (px.totalPeers / 2) + px.totalPeers % 2 {
		px.sendDecide(proposedSequence, number, v)
		return true
	}
	return false
}

func (px *Paxos) sendDecide(preparedSeq int, number int, v interface{}) {
	val := px.history.instances[preparedSeq]
	if val.fate == Decided {
		return
	}
	decideReplies := make(chan DecideReply, px.totalPeers)
	maxDone := make(chan bool, 1)
	requestOwn := &DecideRequest{preparedSeq, v, number, px.history.z_i, px.peers[px.me]}
	replyOwn := &DecideReply{}
	px.Decide(requestOwn, replyOwn)
	decideReplies <- *replyOwn
	px.peerInstances[px.peers[px.me]] = replyOwn.MaxFreeable
	for _, peer := range px.peers {
		if peer != px.peers[px.me] {
			peerName := peer
			go func() {
				request := &DecideRequest{preparedSeq, v, number, px.history.z_i, px.peers[px.me]}
				reply := &DecideReply{}
				to := 5 * time.Millisecond
				for !px.isdead() {
					Ok := call(peerName, "Paxos.Decide", request, reply)
					if Ok {
						px.peerInstances[peer] = reply.MaxFreeable
						px.mu.Lock()
						if reply.HighestNumber > px.highestNumber {
							px.highestNumber = reply.HighestNumber
						}
						px.mu.Unlock()
						decideReplies <- *reply
						if len(decideReplies) == (px.totalPeers/2) + 1 {
							maxDone <- true
						}
						break
					}
					time.Sleep(to)
				}
			}()
		}
	}
	<- maxDone
//	px.history.instances[preparedSeq] = Instance{preparedSeq, v, Decided, number}
}

func (px *Paxos) Prepare(args *PrepareRequest, reply *PrepareReply) error {
	//fmt.Println("got prepare request ", px.peers[px.me], " args: ", args)
	//fmt.Println("server: ", px.peers[px.me], " instances: ", px.history.instances)
	px.muPeerInstances.Lock()
	px.peerInstances[args.Peer] = args.MaxFreeable
	px.muPeerInstances.Unlock()
	val, present := px.history.instances[args.Seq]
	if present && val.fate == Decided {
		reply.Seq_Decided = true
		reply.Seq_num = val.n
		reply.Seq_val = val.value
		return nil
	}
	if args.Number > px.n_p {
//		//fmt.Println("got higher number in: ", px.peers[px.me], " args: ", args)
		px.n_p = args.Number
		reply.Ok = true
		reply.Number = args.Number
		reply.Number_Accepted = px.n_a
		reply.MaxFreeable = px.history.z_i
		px.mu.Lock()
		px.highestNumber = args.Number
		px.mu.Unlock()
	} else {
//		//fmt.Println("not higher number: ", px.peers[px.me], " args: ", args)
		reply.Ok = false
		reply.HighestNumber = px.n_p
	}
	reply.Value_Accepted = px.v_a
	go func() {
		px.triggerDone()
	}()
	//fmt.Println("returning from prepare: ", px.peers[px.me])
	return nil
}

func (px *Paxos) Accept(args *AcceptRequest, reply *AcceptReply) error{
	//fmt.Println("got accept request ", px.peers[px.me], " ", args)
	px.muPeerInstances.Lock()
	px.peerInstances[args.Peer] = args.MaxFreeable
	px.muPeerInstances.Unlock()
	val, present := px.history.instances[args.Seq]
	if present && val.fate == Decided {
		reply.Seq_Decided = true
		reply.Seq_num = val.n
		reply.Seq_val = val.value
		return nil
	}
	if args.Number >= px.n_p {
		px.n_p = args.Number
		px.n_a = args.Number
		px.v_a = args.Value
		reply.Ok = true
		reply.Sequence = args.Number
		px.mu.Lock()
		px.highestNumber = args.Number
		px.mu.Unlock()
	} else {
		reply.Ok = false
		reply.HighestNumber = px.n_p
	}
	reply.MaxFreeable = px.history.z_i
	go func() {
		px.triggerDone()
	}()
	return nil
}

func (px *Paxos) Decide(args *DecideRequest, reply *DecideReply) error {
	//fmt.Println("got decide request ", px.peers[px.me], " ", args)
	px.muPeerInstances.Lock()
	px.peerInstances[args.Peer] = args.MaxFreeable
	px.muPeerInstances.Unlock()
	reply.Ok = true
	reply.MaxFreeable = px.history.z_i
	px.history.add(Instance{args.Sequence, args.Value, Decided, args.Number})
	reply.HighestNumber = px.n_p
	go func() {
		px.triggerDone()
	}()
	return nil
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me
	px.peerInstances = make(map[string]int, len(px.peers))
	px.history = &History{}
	px.history.instances = make(map[int]Instance)
	px.history.oldest = MaxInt
	px.history.latest = -1
	px.history.minimumOfAll = -1
	px.history.z_i = -1
	px.n_a = -1
	px.n_p = -1
	px.totalPeers = len(peers)
	px.highestNumber = -1
	// Your initialization code here.

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}


	return px
}
