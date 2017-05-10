package pbservice
import "time"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
	BackupNotReady = "BackupNotReady"
)

type DBServer interface {
	DBGet(args *GetArgs, reply *GetReply, pb *PBServer) error
	DBPutAppend(args *PutAppendArgs, reply *PutAppendReply, pb *PBServer) error
	BackupGet(args *GetArgs, reply *GetReply, pb *PBServer) error
	BackupPutAppend(args *PutAppendArgs, reply *PutAppendReply, pb *PBServer) error
	CopyDB(args *CopyDBArgs, reply *CopyDBReply, pb *PBServer) error
}

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	OP string
	UID int64
	// You'll have to add definitions here.

	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
	Time time.Time
}

type GetArgs struct {
	Key string
	UID int64
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
	Time time.Time
}

type CopyDBArgs struct {
	DB map[string]string
}

type CopyDBReply struct {
	Err Err
}


// Your RPC definitions here.
