package pbservice
import (
	"time"
	"log"
)

func Get(args *GetArgs, reply *GetReply, pb *PBServer) error {
	pb.mu.Lock()
	if val, ok := pb.getRequestCache[args.UID]; ok {
		*reply = val
		pb.mu.Unlock()
		return nil
	}
	val, ok := pb.db[args.Key]
	if ok {
		tempReply := GetReply{OK, val, time.Now()}
		pb.getRequestCache[args.UID] = tempReply
		*reply = tempReply
	} else {
		reply.Err = ErrNoKey
	}
	pb.mu.Unlock()
	return nil
}

func PutAppend(args *PutAppendArgs, reply *PutAppendReply, pb *PBServer) error {
	if val, ok := pb.putAppendRequestCache[args.UID]; ok {
		*reply = val
		return nil
	}
	tempReply := PutAppendReply{}
	var finalValue string
	if args.OP == "Put" {
		finalValue = args.Value
	} else {
		val, ok := pb.db[args.Key]
		if ok {
			finalValue = val + args.Value
		} else {
			finalValue = "" + args.Value
		}
	}
	pb.db[args.Key] = finalValue
	tempReply.Err = OK
	tempReply.Time = time.Now()
	pb.putAppendRequestCache[args.UID] = tempReply
	log.Println("put value for args in cache: ", args)
	log.Println("getting that value: ", pb.putAppendRequestCache[args.UID])
	*reply = tempReply
	return nil
}

func updateView(pb *PBServer) {
	log.Println("in updateview, current: ", pb.currentStatus)
	pastView := *pb.view
	var err error
	temp, err := pb.vs.Ping(pb.view.Viewnum)
	log.Println("new view: ", temp)
	if err == nil && temp.Viewnum >= pb.view.Viewnum{
		*pb.view = temp
		switch pb.me {
		case pb.view.Primary:
			pb.switchToPrimary(&pastView)
		case pb.view.Backup:
			pb.switchToBackup(&pastView)
		default:
			pb.switchToExtra(&pastView)
		}
	} else {
		log.Println("got error while ping: ", err)
	}
	log.Println("status at the end: ", pb.currentStatus)
}
