package pbservice

type BackupDBServer struct {

}

func (bDBServer BackupDBServer) DBGet(args *GetArgs, reply *GetReply, pb *PBServer) error {
	reply.Err = ErrWrongServer
	return nil
}

func (bDBServer BackupDBServer) DBPutAppend(args *PutAppendArgs, reply *PutAppendReply, pb *PBServer) error {
	reply.Err = ErrWrongServer
	return nil
}

func (bDBServer BackupDBServer) BackupGet(args *GetArgs, reply *GetReply, pb *PBServer) error {
	return Get(args, reply, pb)
}

func (bDBServer BackupDBServer) BackupPutAppend(args *PutAppendArgs, reply *PutAppendReply, pb *PBServer) error {
	pb.mu.Lock()
	temp := PutAppend(args, reply, pb)
	pb.mu.Unlock()
	return temp
}

func (bDBServer BackupDBServer) CopyDB(args *CopyDBArgs, reply *CopyDBReply, pb *PBServer) error {
	pb.db = args.DB
	reply.Err = OK
	return nil
}

