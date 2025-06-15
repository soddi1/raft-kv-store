package raftkv

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key            string
	Value          string
	Op             string // "Put" or "Append"
	SequenceNumber int
	ClientId       int64
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key      string
	SeqNo    int
	ClientId int64
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}
