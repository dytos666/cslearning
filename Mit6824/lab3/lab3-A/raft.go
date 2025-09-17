package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// 日志颜色
const (
	ColorReset  = "\033[0m"
	ColorRed    = "\033[31m"
	ColorGreen  = "\033[32m"
	ColorYellow = "\033[33m"
	ColorBlue   = "\033[34m"
)

type Log struct {
	Term int
	Cmd  string
}
type AppendEntries struct {
	C_ chan AppendEntriesReply
}

const (
	Leader    = 0
	Candidate = 1
	Follower  = 2
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	term        int
	votedFor    int
	log         []Log
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	time_       time.Time
	C_          chan RequestVoteReply

	state int
	lock_ sync.Mutex //protect my variable
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.lock_.Lock()
	term = rf.term
	isleader = rf.state == Leader
	rf.lock_.Unlock()
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
	// Your code here (3C).
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
	// Your code here (3C).
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

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVoteHelper() {
	for rf.killed() == false {
		rf.lock_.Lock()
		rf.time_ = time.Now()
		expire := 200 + (rand.Int63() % 400)
		//fmt.Printf(" %v \n", expire)
		if rf.state != Candidate {
			rf.lock_.Unlock()
			return
		}
		rf.term++
		rf.votedFor = -1
		DPrintfln("[G]", ColorGreen, "term:%d have chance to be leader name%d\n", rf.term, rf.me)

		n := 1
		rf.votedFor = rf.me
		args := RequestVoteArgs{
			Term:         rf.term,
			CandidateId:  rf.me,
			LastLogIndex: rf.lastApplied,
			LastLogTerm:  rf.log[rf.lastApplied].Term,
		}
		rf.lock_.Unlock()
		rf.mu.Lock()
		for i, _ := range rf.peers {
			reply := RequestVoteReply{}
			go rf.sendRequestVote(i, &args, &reply)
		}
		rf.mu.Unlock()

		timeout := time.After(time.Duration(expire) * time.Millisecond)
		timeoutFlag := false
		for n < len(rf.peers)/2+1 && !timeoutFlag {
			//DPrintfln("[C]", ColorBlue, " term:%d get vote num :%d    name:%d\n", rf.term, n, rf.me)
			rf.lock_.Lock()
			if rf.state != Candidate {
				rf.lock_.Unlock()
				return
			}
			rf.lock_.Unlock()
			select {
			case reply := <-rf.C_:
				rf.lock_.Lock()
				if reply.Term != rf.term {
					if reply.Term > rf.term {
						rf.term = reply.Term
						rf.state = Follower
						rf.votedFor = -1
						DPrintfln("[C]", ColorRed, "name:%d args term:%d   rf.term:%d    become follower%v\n", rf.me, args.Term, rf.term)
						rf.lock_.Unlock()
						return
					}
					rf.lock_.Unlock()
					continue
				}
				if reply.VoteGranted {
					n++
					DPrintfln("[C]", ColorBlue, " term:%d get vote num :%d    name:%d\n", rf.term, n, rf.me)
				}
				rf.lock_.Unlock()
			case <-timeout:
				//DPrintfln("[W]", ColorRed, "name:%d   term: %d  time out %v", rf.me, rf.term, expire)
				timeoutFlag = true
				break
			}
		}
		rf.lock_.Lock()
		if rf.state == Candidate && n >= len(rf.peers)/2+1 {

			rf.state = Leader
			go rf.startLearder()
			rf.lock_.Unlock()
			return
		}
		rf.lock_.Unlock()

	}
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	//DPrintfln("[d]", ColorYellow, "receive vote request from name:%d  term :%d\n", args.CandidateId, args.Term)

	// if args.Term < rf.term || rf.votedFor || args.LastLogIndex < rf.commitIndex {
	// 	reply.VoteGranted = false
	// 	reply.Term = rf.term
	// 	return
	// }
	rf.lock_.Lock()
	if args.Term > rf.term {
		//
		rf.term = args.Term
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		reply.Term = rf.term
		rf.lock_.Unlock()
		return
	} else if args.Term < rf.term || rf.votedFor != -1 {
		DPrintfln("[C]", ColorBlue, "name:%d args term:%d   rf.term:%d     voteFor:%v\n", rf.me, args.Term, rf.term, rf.votedFor)
		reply.VoteGranted = false
		reply.Term = rf.term
		rf.lock_.Unlock()
		return
	}
	rf.term = args.Term
	rf.votedFor = args.CandidateId

	reply.Term = rf.term
	reply.VoteGranted = true
	rf.lock_.Unlock()
	//DPrintfln("[d]", ColorYellow, "send vote request to name:%d  term :%d\n", args.CandidateId, args.Term)
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
	if server == rf.me {
		return false
	}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		rf.C_ <- *reply
	}
	//DPrintfln("%s[S] %s\n", ColorGreen, "request Vote\n")

	return ok
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

	// Your code here (3B).

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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	rf.time_ = time.Now()
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// rf.lock_.Lock()

		// rf.lock_.Unlock()
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		rf.lock_.Lock()
		if time.Since(rf.time_) > time.Duration(ms)*time.Millisecond && rf.state != Leader {

			rf.state = Candidate
			rf.lock_.Unlock()
			rf.RequestVoteHelper()
		} else {
			rf.lock_.Unlock()
		}
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{
		term:        0,
		votedFor:    -1,
		log:         make([]Log, 100),
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make([]int, len(peers)),
		matchIndex:  make([]int, len(peers)),
		time_:       time.Now(),

		C_:    make(chan RequestVoteReply),
		state: Follower,
		lock_: sync.Mutex{},
	}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	DPrintfln("[C]", ColorBlue, "   Make a new Raft\n")
	//go rf.RequestVoteHelper()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) startLearder() {

	//DPrintfln("[C]", ColorBlue, "   start a Leader state: %d   name:%d   term :%d\n", rf.state, rf.me, rf.term)

	for rf.killed() == false {
		rf.lock_.Lock()
		if rf.state != Leader {
			rf.lock_.Unlock()
			return
		}
		rf.time_ = time.Now()

		for i, _ := range rf.peers {
			args := AppendEntriesArgs{
				Term:     rf.term,
				LeaderId: rf.me,
				// PrevLogIndex: rf.nextIndex[i],
				// PrevLogTerm:  rf.log[rf.nextIndex[i]].Term,
				// Entries:      rf.log[rf.nextIndex[i]:],
				// LeaderCommit: rf.commitIndex,
			}
			reply := AppendEntriesReply{}
			go func(i int, args AppendEntriesArgs, reply AppendEntriesReply) {

				ok := rf.SendAppendEntries(i, &args, &reply)
				rf.lock_.Lock()
				if ok {
					//DPrintfln("[W]", ColorYellow, "send AppendEntriesArgs ok ")

					if reply.Term > rf.term {

						rf.term = reply.Term
						rf.state = Follower
						rf.votedFor = -1
						rf.lock_.Unlock()
						return
					} else if reply.Success == false {
						rf.nextIndex[i]--

					} else {
						//upload
					}

				}
				rf.lock_.Unlock()
			}(i, args, reply)
		}
		rf.lock_.Unlock()

		time.Sleep(50 * time.Millisecond)

	}

}

func (rf *Raft) SendAppendEntries(i int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if i == rf.me {
		return false
	}
	ok := rf.peers[i].Call("Raft.AppendEntries", args, reply)
	return ok
}
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//DPrintfln("[C]", ColorBlue, "   receive .  ppendEntries \n")
	rf.lock_.Lock()
	rf.time_ = time.Now()
	if args.Term < rf.term {
		reply.Success = false
		reply.Term = rf.term
		rf.lock_.Unlock()
		return
	}

	// } else if args.PrevLogIndex > rf.commitIndex || args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
	// 	reply.Success = false
	// 	reply.Term = rf.term
	// 	return
	// }
	// for i, _ := range args.Entries {
	// 	rf.lock_.Lock()
	// 	if rf.commitIndex < args.PrevLogIndex+i {

	// 	} else if rf.log[args.PrevLogIndex+i].Term != args.Entries[i].Term {
	// 		rf.log[args.PrevLogIndex+i] = args.Entries[i]
	// 	}
	// 	rf.lock_.Unlock()
	// }
	// rf.commitIndex = minInt(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
	reply.Success = true

	if args.Term > rf.term {
		rf.votedFor = -1
	}
	rf.term = args.Term
	rf.state = Follower
	reply.Term = rf.term
	rf.lock_.Unlock()

}
