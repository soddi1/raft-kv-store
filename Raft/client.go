package raftkv

import (
	"crypto/rand"
	// "fmt"
	"labrpc"
	"math/big"
	"sync"
)

// struct for a single client and make customer id, sequence number as well
type Clerk struct {
	mu                   sync.Mutex
	servers              []*labrpc.ClientEnd // the server who we are supposed to send request to
	sequenceNumber       int
	clientIdentification int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// initialize new client id
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.sequenceNumber = 0
	ck.clientIdentification = nrand()
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	tempSeqNo := ck.sequenceNumber
	tempClientId := ck.clientIdentification
	ck.mu.Unlock()

	args := GetArgs{
		Key:      key,
		SeqNo:    tempSeqNo,
		ClientId: tempClientId,
	}
	// fmt.Printf("Getting key %s\n", key)

	for {
		for i := range ck.servers {
			reply := GetReply{}
			ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)

			if ok && !reply.WrongLeader && reply.Err == OK {
				ck.sequenceNumber++
				return reply.Value
			}
		}
	}
}

// shared by Put and Append.
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
// op can be "Put" or "Append"
func (ck *Clerk) PutAppend(key string, value string, op string) {

	ck.mu.Lock()
	tempSeqNo := ck.sequenceNumber
	tempClientId := ck.clientIdentification
	ck.mu.Unlock()

	arguments := PutAppendArgs{
		Key:            key,
		Value:          value,
		Op:             op,
		SequenceNumber: tempSeqNo,
		ClientId:       tempClientId,
	}
	// fmt.Printf("%s key %s with value %s\n", op, key, value)
	for {
		for i := range ck.servers {
			reply := PutAppendReply{}
			ok := ck.servers[i].Call("RaftKV.PutAppend", &arguments, &reply)
			if ok && !reply.WrongLeader && reply.Err == OK {
				// fmt.Printf("%d is the apparent leader?\n", i)
				ck.sequenceNumber++
				return
			}
		}
	}
	// You will have to modify this function.
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
