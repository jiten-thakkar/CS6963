package pbservice

type ExtraDBServer struct {

}

func (eDBServer ExtraDBServer) DBGet(args *GetArgs, reply *GetReply, pb *PBServer) error {
	reply.Err = ErrWrongServer
	return nil
}

func (eDBServer ExtraDBServer) DBPutAppend(args *PutAppendArgs, reply *PutAppendReply, pb *PBServer) error {
	reply.Err = ErrWrongServer
	return nil
}

func (eDBServer ExtraDBServer) BackupGet(args *GetArgs, reply *GetReply, pb *PBServer) error {
	reply.Err = ErrWrongServer
	return nil
}

func (eDBServer ExtraDBServer) BackupPutAppend(args *PutAppendArgs, reply *PutAppendReply, pb *PBServer) error {
	reply.Err = ErrWrongServer
	return nil
}

func (eDBServer ExtraDBServer) CopyDB(args *CopyDBArgs, reply *CopyDBReply, pb *PBServer) error {
	reply.Err = ErrWrongServer
	return nil
}