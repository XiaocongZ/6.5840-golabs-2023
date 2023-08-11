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

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"fmt"
)
type LogEntry struct {
	CommandIndex int
	Command      interface{}
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type serverState int

const (
    Follower serverState = iota
    Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state			serverState
	heartBeat		time.Time
	electionTimeout			time.Duration // set to 0.6-1.2 second
	//Persistent state on all servers
	//TODO for 2C
	term			int
	votedFor		int //-1 for none
	log				[]LogEntry
	//Volatile state on all servers
	commitIndex		uint64
	lastApplied		uint64
	//Volatile state on leaders
	nextIndex		[]uint64
	matchIndex		[]uint64
}


func (rf *Raft) SetElectionTimeout() {
	ms := 600 + (rand.Int63() % 1200)
	rf.electionTimeout = time.Duration(ms) * time.Millisecond
}


// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.term
	isleader = (Leader == rf.state)
	rf.mu.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
}

type AppendEntriesReply struct {
	Term int
	Success bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	if rf.term < args.Term || rf.term == args.Term && (rf.votedFor == -1 || rf.votedFor == args.CandidateId){
		//TODO log up-to-date
		rf.votedFor = args.CandidateId
		rf.state = Follower
		rf.heartBeat = time.Now()
		rf.term = args.Term
		reply.Term = args.Term
		reply.VoteGranted =  true
		DPrintf("Node %d Term %d Vote for %d", rf.me, args.Term, args.CandidateId)
	} else {
		reply.Term = rf.term
		reply.VoteGranted =  false
		DPrintf("Node %d Term %d Refuse Vote for %d", rf.me, rf.term, args.CandidateId)
	}
	rf.mu.Unlock()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	if rf.state == Leader {
		switch {
		case rf.term == args.Term:
			DPrintf("Node %d Term %d AppendEntries Split Brain!", rf.me, args.Term)
		case rf.term > args.Term:
			reply.Term = rf.term
			reply.Success = false
		case rf.term < args.Term:
			rf.state = Follower
			rf.heartBeat = time.Now()
			rf.term = args.Term
			rf.votedFor = args.LeaderId //differentiate from election?
			DPrintf("Node %d Term %d become follower from AppendEntries ", rf.me, args.Term)
		}
	} else {
		switch {
		case rf.term > args.Term://an outdated leader
			reply.Term = rf.term
			reply.Success = false
			DPrintf("Node %d Outdated Leader %d Beat", rf.me, args.LeaderId)
		case rf.term == args.Term://correct heartBeat
			DPrintf("Node %d Beat Received", rf.me)
			rf.heartBeat = time.Now()
			reply.Term = rf.term
			reply.Success = true
			//process payload
			if args.Entries == nil {
				//is heartbeat
			} else {
				//not heartbeat
			}
		case rf.term < args.Term://become follower, same as earlier, can merge together
			rf.state = Follower
			rf.heartBeat = time.Now()
			rf.term = args.Term
			rf.votedFor = args.LeaderId //differentiate from election?
			DPrintf("Node %d Term %d become follower from AppendEntries ", rf.me, args.Term)
		}
	}
	rf.mu.Unlock()
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}


func (rf *Raft) beat(term int) {
	DPrintf("Node %d Beat Term %d", rf.me, term)
	for i, _ := range rf.peers {
		//will send for all the peers without checking state, might become follower half way
		if i != rf.me {
			go func (index int, term int){
				args := AppendEntriesArgs{term, rf.me, 0, 0, nil}
				reply := AppendEntriesReply{}
				rf.sendAppendEntries(index, &args, &reply)
				if term < reply.Term {
					//update new term and become follower
					rf.mu.Lock()
					rf.state = Follower
					rf.heartBeat = time.Now()
					rf.term = reply.Term
					rf.votedFor = -1 //don't have LeaderId
					rf.mu.Unlock()
					DPrintf("Node %d Term %d become follower from Beat ", rf.me, args.Term)

				}
				}(i, term)
		}
	}
}
//go routine that runs indefinitely, send AppendEntries with or without payload
//only work in leader state
func (rf *Raft) beatOrSend() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.state == Leader {
			term := rf.term
			rf.mu.Unlock()
			rf.beat(term)
		}else {
			rf.mu.Unlock()
		}
		//no more than 10 heartbeats in 1 second
		time.Sleep(100 * time.Millisecond)
	}

}

func (rf *Raft) elect(term int) {
	//fmt.Println("Start election")
	var electLock sync.Mutex
	c := sync.NewCond(&electLock)
	majority := len(rf.peers) / 2
	finished := 0
	granted := 0
	for i, _ := range rf.peers {
		if i == rf.me {continue}
		go func (index int) {
			args := RequestVoteArgs{term, rf.me}
			reply := RequestVoteReply{}
			rf.sendRequestVote(index, &args, &reply)
			electLock.Lock()
			if reply.VoteGranted == true {
				granted++
			}
			finished++
			c.Broadcast()
			electLock.Unlock()
			//update term possibly
		}(i)
	}
	electLock.Lock()
	for granted < majority && finished < len(rf.peers) {
		c.Wait()
	}

	if granted < majority {
		DPrintf("Failed election, granted = %d", granted)
	} else {
		//TODO do we need to check term number?

		rf.mu.Lock()
		if rf.term == term {
			rf.state = Leader
			DPrintf("Node %d Term %d Elected, granted by %d", rf.me, term, granted)
			rf.mu.Unlock()
			rf.beat(rf.term)
		} else {
			rf.mu.Unlock()
			DPrintf("Node %d Term %d Expired, granted by %d", rf.me, term, granted)
		}

	}
	//prevent data race
	electLock.Unlock()
}


// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	//stop long-running go routines?
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
//Reasons to change state to candidate:
//Election expired
//Receive AppendEntries from new leader
//Others can wait:
//reply to RequestVote
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		timeElapsed := time.Since(rf.heartBeat)
		if rf.state != Leader && timeElapsed > rf.electionTimeout {
			DPrintf("Node %d Term %d Timeout", rf.me, rf.term)
			rf.state = Candidate
			rf.term++
			rf.votedFor = -1
			rf.heartBeat = time.Now()
			//
		}
		term := rf.term
		rf.SetElectionTimeout()

		//start election only when in candidate
		if rf.state == Candidate && rf.votedFor == -1{
			DPrintf("Node %d Run for Term %d", rf.me, term)
			rf.votedFor = rf.me
			go rf.elect(term)
		}
		rf.mu.Unlock()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		//ms := 50 + (rand.Int63() % 300)
		//time.Sleep(time.Duration(ms) * time.Millisecond)
		time.Sleep(20 * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.term = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry,0)

	rf.commitIndex = 0
	rf.lastApplied = 0
	//Volatile state on leaders not needed, will initialize after election
	fmt.Println("Number of peers", len(peers))
	rf.SetElectionTimeout()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	rf.heartBeat = time.Now()
	go rf.ticker()
	go rf.beatOrSend()

	return rf
}
