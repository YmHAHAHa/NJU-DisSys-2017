package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

// import "sync"
// import "labrpc"

// import "bytes"
// import "encoding/gob"
import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

const (
	FOLLOWER  = 0
	CANDIDATE = 1
	LEADER    = 2

	HEARTBEAT_INTERVAL    = 50
	MIN_ELECTION_INTERVAL = 150
	MAX_ELECTION_INTERVAL = 300
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state         int
	timer         *time.Timer
	votesAcquired int
	applyChan     chan ApplyMsg

	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	term = rf.currentTerm
	isleader = (rf.state == LEADER)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)

	if data == nil {
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votesAcquired)
	d.Decode(&rf.log)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	bbit := true
	if len(rf.log) > 0 {
		lastLogTerm := rf.log[len(rf.log)-1].Term
		if lastLogTerm > args.LastLogTerm {
			bbit = false
		} else if lastLogTerm == args.LastLogTerm &&
			len(rf.log)-1 > args.LastLogIndex {
			bbit = false
		}
	}

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term == rf.currentTerm {
		reply.Term = rf.currentTerm
		if rf.votedFor == -1 && bbit {
			rf.votedFor = args.CandidateId
			rf.persist()
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}
		return
	}
	if args.Term > rf.currentTerm {
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.timer.Reset(properTimeDuration(rf.state))
		reply.Term = args.Term
		if bbit {
			rf.votedFor = args.CandidateId
			rf.persist()
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}
		return
	}
	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) solveRequestVoteReply(rvreply RequestVoteReply) {
	//unexpect situation
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != CANDIDATE || rvreply.Term < rf.currentTerm {
		return
	}
	if rvreply.Term > rf.currentTerm {
		rf.currentTerm = rvreply.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.timer.Reset(properTimeDuration(rf.state))
		return
	}

	//normal
	if !rvreply.VoteGranted {
		return
	}
	rf.votesAcquired += 1
	if rf.votesAcquired > len(rf.peers)/2 {
		rf.state = LEADER
		for i, _ := range rf.peers {
			if i == rf.me {
				continue
			}
			rf.matchIndex[i] = -1
			rf.nextIndex[i] = len(rf.log)
		}
		rf.timer.Reset(properTimeDuration(rf.state))
	}
}

type AppendEntryArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntryReply struct {
	Term        int
	Success     bool
	CommitIndex int
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (rf *Raft) CommitLog() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.commitIndex = min(rf.commitIndex, len(rf.log)-1)
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		rf.applyChan <- ApplyMsg{
			Index:   i + 1,
			Command: rf.log[i].Command,
		}
	}

	rf.lastApplied = rf.commitIndex
}

func (rf *Raft) AppendEntry(args AppendEntryArgs, reply *AppendEntryReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	rf.state = FOLLOWER
	rf.currentTerm = args.Term
	rf.votedFor = -1
	reply.Term = args.Term

	if args.PrevLogIndex >= 0 &&
		(len(rf.log)-1 < args.PrevLogIndex ||
			rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
		reply.Success = false
		reply.CommitIndex = min(len(rf.log)-1, args.PrevLogIndex)
		for reply.CommitIndex >= 0 &&
			rf.log[reply.CommitIndex].Term != args.PrevLogTerm {
			reply.CommitIndex--
		}
	} else if args.Entries != nil {
		rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
		if len(rf.log) > args.LeaderCommit {
			rf.commitIndex = args.LeaderCommit
			//TODO:commitlog
			go rf.CommitLog()
		}
		reply.CommitIndex = len(rf.log) - 1
		reply.Success = true
	} else {
		if len(rf.log) > args.LeaderCommit {
			rf.commitIndex = args.LeaderCommit
			//TODO:commitlog
			go rf.CommitLog()
		}
		reply.CommitIndex = rf.commitIndex
		reply.Success = true
	}
	rf.persist()
	rf.timer.Reset(properTimeDuration(rf.state))
}

func (rf *Raft) sendAppendEntry(server int, args AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

func (rf *Raft) solveAppendEntryReply(server int, aereply AppendEntryReply) {
	//TODO
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//unexpect
	if rf.state != LEADER {
		return
	}
	if aereply.Term > rf.currentTerm {
		rf.currentTerm = aereply.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.timer.Reset(properTimeDuration(rf.state))
		return
	}

	//normal
	if aereply.Success {
		rf.matchIndex[server] = aereply.CommitIndex
		rf.nextIndex[server] = aereply.CommitIndex + 1
		num := 1
		for i, _ := range rf.peers {
			if i == rf.me {
				continue
			}
			if rf.matchIndex[i] >= rf.matchIndex[server] {
				num++
			}
		}
		if num > len(rf.peers)/2 &&
			rf.commitIndex < rf.matchIndex[server] &&
			rf.log[rf.matchIndex[server]].Term == rf.currentTerm {
			rf.commitIndex = rf.matchIndex[server]
			go rf.CommitLog()
		}
	} else {
		rf.nextIndex[server] = aereply.CommitIndex + 1
		rf.sendAppendEntryToFollowers()
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := -1
	isLeader := false

	if rf.state == LEADER {
		term = rf.currentTerm
		isLeader = true
		rf.log = append(
			rf.log,
			LogEntry{
				Index:   index,
				Term:    term,
				Command: command,
			})
		index = len(rf.log)
		rf.persist()
	}

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) changeToC() {
	if rf.state == CANDIDATE {
		return
	}
	rf.state = CANDIDATE
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.votesAcquired = 1
	rf.persist()
	rvargs := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		// LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	if len(rf.log) > 0 {
		rvargs.LastLogTerm = rf.log[len(rf.log)-1].Term
	}

	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(p int) {
			var rvreply RequestVoteReply
			if rf.sendRequestVote(p, rvargs, &rvreply) {
				rf.solveRequestVoteReply(rvreply)
			}
		}(i)
	}
	rf.timer.Reset(properTimeDuration(rf.state))
}

func (rf *Raft) sendAppendEntryToFollowers() {
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		aeargs := AppendEntryArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.nextIndex[i] - 1,
			LeaderCommit: rf.commitIndex,
		}
		if rf.nextIndex[i] > 0 {
			aeargs.PrevLogTerm = rf.log[aeargs.PrevLogIndex].Term
		}
		if rf.nextIndex[i] < len(rf.log) {
			aeargs.Entries = rf.log[rf.nextIndex[i]:]
		}

		go func(f int) {
			var aereply AppendEntryReply
			if rf.sendAppendEntry(f, aeargs, &aereply) {
				rf.solveAppendEntryReply(f, aereply)
			}
		}(i)
	}
}

func (rf *Raft) keepAuthority() {
	rf.sendAppendEntryToFollowers()
	rf.timer.Reset(properTimeDuration(rf.state))
}

func (rf *Raft) solveTimeOut() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	switch rf.state {
	case LEADER:
		rf.keepAuthority()
	case CANDIDATE:
		rf.state = FOLLOWER
		rf.changeToC()
	case FOLLOWER:
		rf.changeToC()
	}
}

func properTimeDuration(state int) time.Duration {
	if state == LEADER {
		return time.Millisecond * HEARTBEAT_INTERVAL
	}
	return time.Millisecond * time.Duration(MIN_ELECTION_INTERVAL+rand.Intn(MAX_ELECTION_INTERVAL-MIN_ELECTION_INTERVAL))
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.state = FOLLOWER
	rf.applyChan = applyCh

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// rf.initTimer()
	// if rf.timer == nil {
	rf.timer = time.NewTimer(properTimeDuration(rf.state))
	go func() {
		for {
			<-rf.timer.C
			rf.solveTimeOut()
		}
	}()
	// }
	// rf.timer.Reset(timeout)

	return rf
}
