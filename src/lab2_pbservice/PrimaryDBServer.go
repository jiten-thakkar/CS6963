package pbservice
import (
	"fmt"
	"log"
)

type PrimaryDBServer struct {
	Err string
}

func (pDBServer PrimaryDBServer) Error() string {
	return fmt.Sprintf(pDBServer.Err)
}

func (pDBServer PrimaryDBServer) DBGet(args *GetArgs, reply *GetReply, pb *PBServer) error {
	log.Println("server: ", pb.me)
	if val, ok := pb.getRequestCache[args.UID]; ok {
		*reply = val
		return nil
	}
	if pb.view.Backup != "" {
		log.Println("got backup as: ", pb.view.Backup)
		if !pb.backupInSync {
			reply.Err = BackupNotReady
			return nil
		} else {
			for {
				ok := call(pb.view.Backup, "PBServer.BackupGetPB", args, reply)
				if ok {
					break
				} else {
					log.Println("calling updateview from primary DBGet because getting timeout from backup")
					updateView(pb)
					return pb.DBGet(args, reply, pb)
//					if pb.view.Primary == pb.me {
//						return pb.DBGet(args, reply, pb)
//					} else {
//						repl
//						return nil
//					}
				}
			}
			if reply.Err == ErrWrongServer {
				log.Println("calling updateview from primary DBGet because got wrong server error from backup")
				updateView(pb)
//				if pb.view.Primary == pb.me {
					return pb.DBGet(args, reply, pb)
//				}
			}
//			ok := call(pb.view.Backup, "PBServer.BackupGetPB", args, reply)
//			if ok {
//				if reply.Err == ErrWrongServer {
//					log.Println("calling updateview from primary DBGet because got wrong server error from backup")
//					updateView(pb)
//					if pb.view.Primary == pb.me {
//						return pb.DBGet(args, reply, pb)
//					}
//				} else if reply.Err == ErrNoKey {
//					return nil
//				} else {
//					log.Println("now calling regular get")
//					return Get(args, reply, pb)
//				}
//			} else {
//				return PrimaryDBServer{"couldn't reach backup"}
//			}
		}
	}
	log.Println("now calling regular get")
	return Get(args, reply, pb)
}

func (pDBServer PrimaryDBServer) DBPutAppend(args *PutAppendArgs, reply *PutAppendReply, pb *PBServer) error {
	pb.mu.Lock()
//	log.Println("value in cache: ", pb.putAppendRequestCache[args.UID])
	if val, ok := pb.putAppendRequestCache[args.UID]; ok {
//		log.Println("found value in cache for args: ", args)
		*reply = val
		pb.mu.Unlock()
		return nil
	}
	if pb.view.Backup != "" {
		if !pb.backupInSync {
			reply.Err = BackupNotReady
			pb.mu.Unlock()
			return nil
		} else {
			for {
				ok := call(pb.view.Backup, "PBServer.BackupPutAppendPB", args, reply)
				if ok {
					break
				} else {
					log.Println("calling updateview from primary DBPutAppend because getting timeout from backup")
					updateView(pb)
//					if pb.view.Primary == pb.me {
						pb.mu.Unlock()
						return pb.DBPutAppend(args, reply, pb)
//					}
				}
			}
			if reply.Err == ErrWrongServer {
				log.Println("calling updateview from primary DBPutAppend because got wrong server error from backup")
				updateView(pb)
//				if pb.view.Primary == pb.me {
					pb.mu.Unlock()
					return pb.DBPutAppend(args, reply, pb)
//				}
			}
//			ok := call(pb.view.Backup, "PBServer.BackupPutAppendPB", args, reply)
//			if ok {
//				if reply.Err == ErrWrongServer {
//					log.Println("calling updateview from primary DBPutAppend because got wrong server error from backup")
//					updateView(pb)
//					if pb.view.Primary == pb.me {
//						pb.mu.Unlock()
//						return pb.DBPutAppend(args, reply, pb)
//					}
//				} else {
//					log.Println("now calling regular put")
//					temp := PutAppend(args, reply, pb)
//					pb.mu.Unlock()
//					return temp
//				}
//			} else {
//				pb.mu.Unlock()
//				return PrimaryDBServer{"couldn't reach backup"}
//			}
		}
	}
	temp := PutAppend(args, reply, pb)
	pb.mu.Unlock()
	return temp
}

func (pDBServer PrimaryDBServer) BackupGet(args *GetArgs, reply *GetReply, pb *PBServer) error {
	reply.Err = ErrWrongServer
	return nil
}

func (pDBServer PrimaryDBServer) BackupPutAppend(args *PutAppendArgs, reply *PutAppendReply, pb *PBServer) error {
	reply.Err = ErrWrongServer
	return nil
}

func (pDBServer PrimaryDBServer) CopyDB(args *CopyDBArgs, reply *CopyDBReply, pb *PBServer) error{
	reply.Err = ErrWrongServer
	return nil
}