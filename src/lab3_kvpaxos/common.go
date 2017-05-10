package kvpaxos

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
	GET = "get"
	PUT = "put"
	APPEND = "append"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	UID   int64
	LastReceived int64
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	UID   int64
	LastReceived int64
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}
