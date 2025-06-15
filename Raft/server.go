package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// represents a single log entry send to start in raft
type Op struct {
	Key            string
	Value          string
	Operation      string
	SequenceNumber int
	ClientId       int64
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	kvStore    map[string]string
	logEntries []Op
}

// client calls this
func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	newOp := Op{
		Key:            args.Key,
		Value:          "",
		Operation:      "Get",
		SequenceNumber: args.SeqNo,
		ClientId:       args.ClientId,
	}
	kv.mu.Lock()
	for _, existingEntry := range kv.logEntries {
		if existingEntry.SequenceNumber == newOp.SequenceNumber &&
			existingEntry.ClientId == newOp.ClientId {
			reply.WrongLeader = false
			reply.Err = OK
			reply.Value = existingEntry.Value
			kv.mu.Unlock()
			return
		}
	}
	kv.mu.Unlock()

	idx, term, leader := kv.rf.Start(newOp)
	if idx == -1 && term == -1 && !leader {
		reply.WrongLeader = true
		return
	}
	getChannel := make(chan GetReply)
	putChannel := make(chan PutAppendReply)
	go kv.readChan(idx, putChannel, getChannel)
	select {
	case x := <-getChannel:
		getReq := x
		reply.WrongLeader = getReq.WrongLeader
		reply.Err = getReq.Err
		reply.Value = getReq.Value
		return
	case <-time.After(time.Millisecond * 3000):
		reply.WrongLeader = false
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

	newOp := Op{
		Key:            args.Key,
		Value:          args.Value,
		Operation:      args.Op,
		SequenceNumber: args.SequenceNumber,
		ClientId:       args.ClientId,
	}
	kv.mu.Lock()
	for _, existingEntry := range kv.logEntries {
		// Check if the entry has the same SequenceNumber and ClientId
		if existingEntry.SequenceNumber == newOp.SequenceNumber &&
			existingEntry.ClientId == newOp.ClientId {
			reply.WrongLeader = false
			reply.Err = OK
			kv.mu.Unlock()
			return
		}
	}
	kv.mu.Unlock()

	idx, term, leader := kv.rf.Start(newOp)
	if idx == -1 && term == -1 && !leader {
		reply.WrongLeader = true
		reply.Err = ErrNoKey
		return
	}

	putChannel := make(chan PutAppendReply)
	getChannel := make(chan GetReply)
	go kv.readChan(idx, putChannel, getChannel)
	select {
	case x := <-putChannel:
		putReq := x
		reply.WrongLeader = putReq.WrongLeader
		reply.Err = putReq.Err

		return
	case <-time.After(time.Millisecond * 3000):
		reply.WrongLeader = false
	}
}

// the tester calls Kill() when a RaftKV instance won't
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.applyCh = make(chan raft.ApplyMsg, 100)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.kvStore = make(map[string]string)
	kv.logEntries = make([]Op, 0)

	return kv
}

func (kv *RaftKV) readChan(idx int, channel chan PutAppendReply, channelTwo chan GetReply) {
	x := <-kv.applyCh
	newOp := x.Command.(Op)

	if x.Index != idx {
		return
	}

	switch newOp.Operation {
	case "Get":
		kv.mu.Lock()
		value, exists := kv.kvStore[newOp.Key]
		// kv.mu.Unlock()
		if exists {
			kv.mu.Unlock()
			channelTwo <- GetReply{
				WrongLeader: false,
				Err:         OK,
				Value:       value,
			}
			kv.mu.Lock()
		} else {
			kv.mu.Unlock()
			channelTwo <- GetReply{
				WrongLeader: false,
				Err:         ErrNoKey,
			}
			kv.mu.Lock()
		}
		// kv.mu.Lock()
		newOp.Value = value
		kv.logEntries = append(kv.logEntries, newOp)
		kv.mu.Unlock()
		return
	case "Put":
		kv.mu.Lock()
		kv.kvStore[newOp.Key] = newOp.Value
		kv.mu.Unlock()
		channel <- PutAppendReply{
			WrongLeader: false,
			Err:         OK,
		}
		kv.mu.Lock()
		kv.logEntries = append(kv.logEntries, newOp)
		kv.mu.Unlock()
		return
	case "Append":
		kv.mu.Lock()
		_, exists := kv.kvStore[newOp.Key]
		// kv.mu.Unlock()
		if exists {
			currentValue := kv.kvStore[newOp.Key]
			kv.kvStore[newOp.Key] = currentValue + newOp.Value
		} else {
			kv.kvStore[newOp.Key] = newOp.Value
		}
		kv.mu.Unlock()
		channel <- PutAppendReply{
			WrongLeader: false,
			Err:         OK,
		}
		kv.mu.Lock()
		kv.logEntries = append(kv.logEntries, newOp)
		kv.mu.Unlock()
		return
	}
}
