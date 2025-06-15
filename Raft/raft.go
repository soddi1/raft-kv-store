package raft

import (
	"bytes"
	"encoding/gob"

	"labrpc"
	"math/rand"
	"sync"
	"time"
)

type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool
	Snapshot    []byte
}

type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int

	votedFor      int
	currentTerm   int
	role          int
	counter       int
	heartBeat     chan int
	log           []LogEntry
	commitIndex   int
	lastApplied   int
	matchingIndex []int
	// nextIndex     []int
}

func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	
	rf.mu.Lock()
	term = rf.currentTerm
	if rf.role == 1 {
		isleader = true
	}
	rf.mu.Unlock()
	return term, isleader
}

func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.log)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.commitIndex)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	rf.mu.Lock()
	d.Decode(&rf.log)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.commitIndex)
	rf.mu.Unlock()
}

// example RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	rf.persist()
	temp := rf.currentTerm
	rf.mu.Unlock()
	if args.Term >= temp {
		rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.role = -1
		rf.counter = 0
		rf.persist()
		rf.mu.Unlock()
	}

	rf.mu.Lock()
	rf.persist()
	temp = rf.currentTerm
	tempTwo := rf.votedFor
	currLog := rf.log
	currLastLogIndexTwo := len(rf.log)
	currLastLogTermTwo := -1
	if currLastLogIndexTwo > 0 {
		currLastLogTermTwo = currLog[currLastLogIndexTwo-1].Term
	}
	rf.mu.Unlock()

	if args.Term == temp && !(currLastLogTermTwo > args.LastLogTerm || (currLastLogTermTwo == args.LastLogTerm && currLastLogIndexTwo > args.LastLogIndex)) {
		if tempTwo == -1 {
			rf.mu.Lock()
			rf.votedFor = args.CandidateID
			reply.VoteGranted = true
			rf.persist()
			rf.mu.Unlock()
		}
		rf.mu.Lock()
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
	}

	rf.mu.Lock()
	rf.persist()
	temp = rf.currentTerm
	rf.mu.Unlock()

	if args.Term < temp {
		rf.mu.Lock()
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.votedFor = -1
		rf.persist()
		rf.mu.Unlock()
		return
	}
	rf.heartBeat <- 1
	rf.mu.Lock()
	rf.persist()
	rf.mu.Unlock()
}

func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type RequestAppendArgs struct {
	Term         int
	LeaderID     int
	Log          []LogEntry
	LeaderCommit int
	LastLogIndex int
	LastLogTerm  int
}

type RequestAppendReply struct {
	Term         int
	Success      bool
	MatchedIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

func (rf *Raft) AppendEntries(args RequestAppendArgs, reply *RequestAppendReply) {
	rf.mu.Lock()
	// fmt.Printf("Server %d getting append entry from %d\n", rf.me, args.LeaderID)
	rf.persist()
	temp := rf.currentTerm
	tempTwo := rf.role
	currLog := rf.log
	currLastLogIndexTwo := len(rf.log)
	currLastLogTermTwo := -1
	if currLastLogIndexTwo > 0 {
		currLastLogTermTwo = currLog[currLastLogIndexTwo-1].Term
	}
	rf.mu.Unlock()

	// fmt.Printf("Server %d sent %d a heartbeat with MatchedIndex %d\n", args.LeaderID, rf.me, len(args.Log))
	// fmt.Printf("The current commit index for server %d before append entry is %d and its last index is: %d\n", currMe, currCommit, currLastLogIndexTwo)
	if currLastLogTermTwo > args.LastLogTerm || (currLastLogTermTwo == args.LastLogTerm && currLastLogIndexTwo > args.LastLogIndex) {
		// rf.mu.Lock()
		// fmt.Printf("%d does not accept log index %d from server %d\n", rf.me, len(rf.log), args.LeaderID)
		// rf.mu.Unlock()
		rf.mu.Lock()
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.persist()
		rf.mu.Unlock()
		return
	}
	if args.Term > temp && tempTwo != -1 {
		rf.mu.Lock()
		rf.role = -1
		rf.counter = 0
		rf.votedFor = -1
		rf.currentTerm = args.Term
		temp = args.Term
		rf.persist()

		rf.mu.Unlock()
	}
	if args.Term >= temp {
		rf.mu.Lock()
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		// fmt.Printf("before copying %d\n", rf.log)
		rf.log = args.Log
		reply.MatchedIndex = len(rf.log)
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, len(rf.log))
		}
		// fmt.Printf("server %d saved log %d in term %d and commit index of this server is %d\n", rf.me, rf.log, rf.currentTerm, rf.commitIndex)
		rf.persist()
		reply.Success = true

		rf.mu.Unlock()
		rf.heartBeat <- 1
	}
	if temp > args.Term {
		rf.mu.Lock()
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.mu.Unlock()
		rf.mu.Lock()
		rf.persist()
		rf.mu.Unlock()
		return
	}
	rf.mu.Lock()
	rf.persist()
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args RequestAppendArgs, reply *RequestAppendReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	rf.mu.Lock()
	currRole := rf.role
	rf.mu.Unlock()
	if currRole == -1 || currRole == 0 {
		return -1, -1, false
	} else {
		rf.mu.Lock()
		term = rf.currentTerm
		currTerm := rf.currentTerm
		rf.mu.Unlock()
		entry := LogEntry{
			Term:    currTerm,
			Command: command,
		}
		rf.mu.Lock()
		rf.log = append(rf.log, entry)
		// fmt.Printf("The log %d coming at server %d with the log: %d and commit index: %d\n", command, rf.me, rf.log, rf.commitIndex)
		rf.persist()
		index = len(rf.log)
		isLeader = true

		// fmt.Printf("the current commitindex is %d\n", rf.commitIndex)
		rf.mu.Unlock()
		return index, term, isLeader
	}

}

func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.role = -1 //start as follower
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.counter = 0
	rf.lastApplied = 0
	rf.heartBeat = make(chan int)
	rf.commitIndex = 0
	rf.matchingIndex = make([]int, len(peers))
	// Your initialization code here.
	go rf.heartBeatTimer()
	go rf.applyEntries(applyCh)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.mu.Lock()
	rf.persist()
	rf.mu.Unlock()
	return rf
}

func (rf *Raft) applyEntries(applyCh chan ApplyMsg) {
	for {
		time.Sleep(50 * time.Millisecond)
		rf.mu.Lock()
		if len(rf.log) > 0 && rf.lastApplied < len(rf.log) && rf.lastApplied < rf.commitIndex {
			message := ApplyMsg{
				Index:   rf.lastApplied + 1,
				Command: rf.log[rf.lastApplied].Command,
			}
			rf.mu.Unlock()
			applyCh <- message
			rf.mu.Lock()
			rf.lastApplied++
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) heartBeatPing() {
	usedElements := make(map[int]struct{})

	for {
		rf.mu.Lock()
		temp := rf.role
		rf.mu.Unlock()

		if temp == 1 {
			time.Sleep(50 * time.Millisecond)

			for i := range rf.peers {
				rf.mu.Lock()
				currRole := rf.role
				rf.mu.Unlock()
				if currRole == 1 {
					go func(peerIndex int) {
						rf.sendPing(peerIndex)
					}(i)
				}
			}
			rf.mu.Lock()
			currRole := rf.role
			rf.mu.Unlock()
			if currRole != 1 {
				break
			}
			rf.mu.Lock()
			rf.matchingIndex[rf.me] = len(rf.log)
			rf.mu.Unlock()
			frequencyMap := make(map[int]int)

			rf.mu.Lock()
			for _, num := range rf.matchingIndex {
				frequencyMap[num]++
			}

			requiredVotes := len(rf.matchingIndex)/2 + 1
			for num, frequency := range frequencyMap {
				if frequency >= requiredVotes {
					if _, used := usedElements[num]; !used {
						if len(rf.log) > 0 && rf.currentTerm == rf.log[len(rf.log)-1].Term {
							rf.commitIndex = num
							usedElements[num] = struct{}{}
						}
						// fmt.Printf("The new commit index is: %d and the majority array is: %d\n", rf.commitIndex, rf.matchingIndex)
						rf.persist()
					}
				}
			}

			rf.persist()
			rf.mu.Unlock()

		}
	}
}

func (rf *Raft) heartBeatTimer() {
	for {
		rf.mu.Lock()
		temp := rf.role
		rf.mu.Unlock()
		if temp == 1 {
			continue
		}
		electionTimeout := rand.Intn(300) + 600
		timer := time.NewTimer(time.Duration(electionTimeout) * time.Millisecond)
		select {
		case <-timer.C:
			timer.Stop()
			rf.mu.Lock()
			rf.role = 0
			rf.votedFor = rf.me
			rf.counter = 0
			rf.persist()
			rf.mu.Unlock()
			go rf.callElection()

		case <-rf.heartBeat:
			timer.Stop()
			continue
		}
	}
}

func (rf *Raft) callElection() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.persist()
	rf.mu.Unlock()
	for i := range rf.peers {
		rf.mu.Lock()
		temp := rf.me
		rf.mu.Unlock()
		if i == temp {
			continue
		} else {
			go rf.sendVoteRequests(i)
		}
	}
}

func (rf *Raft) sendVoteRequests(i int) {
	rf.mu.Lock()
	// fmt.Printf("Server %d Sending vote request to: %d\n", rf.me, i)
	rf.persist()
	if rf.counter == 0 {
		rf.counter++
	}
	currTerm := rf.currentTerm
	meTemp := rf.me
	currLastLogIndex := len(rf.log)
	currLastLogTerm := -1
	if currLastLogIndex > 0 {
		currLastLogTerm = rf.log[len(rf.log)-1].Term
	}
	rf.mu.Unlock()
	arguments := RequestVoteArgs{
		Term:         currTerm,
		CandidateID:  meTemp,
		LastLogIndex: currLastLogIndex,
		LastLogTerm:  currLastLogTerm,
	}
	replyArgs := RequestVoteReply{
		Term:        0,
		VoteGranted: false,
	}
	accept := rf.sendRequestVote(i, arguments, &replyArgs)
	if accept {
		rf.mu.Lock()
		// fmt.Printf("%d has %d many votes\n", rf.me, rf.counter)
		currTermTwo := rf.currentTerm
		countTemp := rf.counter
		rf.mu.Unlock()
		if replyArgs.Term > currTermTwo {
			rf.mu.Lock()
			rf.role = -1
			rf.counter = 0
			rf.votedFor = -1
			rf.currentTerm = replyArgs.Term
			rf.persist()
			// rf.heartBeat <- 1
			rf.mu.Unlock()
			return
		}
		rf.mu.Lock()
		currRole := rf.role
		rf.mu.Unlock()
		if replyArgs.VoteGranted && currRole == 0 {
			rf.mu.Lock()
			rf.counter++
			countTemp = rf.counter
			rf.persist()
			// fmt.Printf("%d received vote from %d\n", rf.me, i)
			rf.mu.Unlock()
		}
		rf.mu.Lock()
		currRole = rf.role
		rf.mu.Unlock()
		if countTemp > len(rf.peers)/2 && currRole != 1 {
			rf.mu.Lock()
			rf.role = 1
			rf.counter = 0
			rf.votedFor = -1
			rf.persist()
			rf.mu.Unlock()
			// fmt.Printf("%d is the leader with log %d\n", rf.me, rf.log)
			go rf.heartBeatPing()
			return
		}

	}
	rf.mu.Lock()
	rf.persist()
	rf.mu.Unlock()
}

func (rf *Raft) sendPing(i int) {
	rf.mu.Lock()
	termTemp := rf.currentTerm
	meTemp := rf.me
	currLogs := rf.log
	currCommit := rf.commitIndex
	currLastLogIndex := len(rf.log)
	currLastLogTerm := -1
	if currLastLogIndex > 0 {
		currLastLogTerm = rf.log[len(rf.log)-1].Term
	}
	rf.mu.Unlock()
	argsuments := RequestAppendArgs{
		Term:         termTemp,
		LeaderID:     meTemp,
		Log:          currLogs,
		LeaderCommit: currCommit,
		LastLogIndex: currLastLogIndex,
		LastLogTerm:  currLastLogTerm,
	}
	entriesReply := RequestAppendReply{
		Term:         0,
		Success:      false,
		MatchedIndex: 0,
	}
	accept := rf.sendAppendEntries(i, argsuments, &entriesReply)
	if accept {
		rf.mu.Lock()
		currTermTwo := rf.currentTerm
		rf.mu.Unlock()
		if entriesReply.Term > currTermTwo {
			rf.mu.Lock()
			rf.currentTerm = entriesReply.Term
			rf.role = -1
			rf.counter = 0
			rf.votedFor = -1
			rf.persist()
			rf.mu.Unlock()
			return
		}
		if entriesReply.Success {
			rf.mu.Lock()
			rf.matchingIndex[i] = entriesReply.MatchedIndex
			// fmt.Printf("matched index list: %d\n", rf.matchingIndex)
			// fmt.Printf("Matched index recieved: %d\n", entriesReply.MatchedIndex)
			rf.mu.Unlock()
		}
	}
	rf.mu.Lock()
	rf.persist()
	rf.mu.Unlock()
}
