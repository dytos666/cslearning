package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"

	"6.5840/labgob"
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
	Cmd  any
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
	//mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan raftapi.ApplyMsg

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	term        int
	votedFor    int
	log         []Log
	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int
	time_      time.Time
	C_         chan RequestVoteReply

	state                 int
	lock_                 sync.Mutex //protect my variable
	last_snapShot_commit_ int
	last_snapShot_term_   int
	snapshot              []byte
	cond                  *sync.Cond
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.term)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.last_snapShot_commit_)
	e.Encode((rf.last_snapShot_term_))
	//DPrintfln("[C]", ColorBlue, "persist  term:%d voteful:%v  loglen:%d,     %d\n", rf.term, rf.votedFor, rf.lastApplied, len(rf.log[:rf.lastApplied+1]))
	// DPrintfln("[C]", ColorBlue, "persist name:%d term:%d voteful:%v  loglen:%d  \n", rf.me, rf.term, rf.votedFor, len(rf.log))
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var voteFor int
	var log []Log
	var last_snapShot_commit_ int
	var last_snapShot_term_ int

	if d.Decode(&term) != nil ||
		d.Decode(&voteFor) != nil || d.Decode(&log) != nil || d.Decode(&last_snapShot_commit_) != nil || d.Decode(&last_snapShot_term_) != nil {
		panic("readPersist: decode error")
	} else {
		rf.lock_.Lock()
		rf.term = term
		rf.votedFor = voteFor
		rf.log = log
		rf.last_snapShot_commit_ = last_snapShot_commit_
		rf.last_snapShot_term_ = last_snapShot_term_
		rf.snapshot = rf.persister.ReadSnapshot()
		rf.commitIndex = rf.last_snapShot_commit_
		rf.lastApplied = rf.commitIndex
		//DPrintfln("[C]", ColorBlue, "read  term:%d voteful:%v  loglen:%d\n", rf.term, rf.votedFor, rf.lastApplied)
		// DPrintfln("[C]", ColorBlue, "read name:%d term:%d voteful:%v  loglen:%d\n", rf.me, rf.term, rf.votedFor, len(rf.log))

		rf.lock_.Unlock()
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.lock_.Lock()
	defer rf.lock_.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.lock_.Lock()
	defer rf.lock_.Unlock()
	if rf.last_snapShot_commit_ >= index {
		return
	}
	// DPrintfln("[C]", ColorBlue, "name:%d snapshot index:%d , loglen:%d, last_snapShot_commit_:%d\n", rf.me, index, len(rf.log), rf.last_snapShot_commit_)

	rf.last_snapShot_term_ = rf.log[rf.indexSub(index)].Term
	rf.log = rf.log[rf.indexSub(index):]
	rf.log[0].Cmd = ""
	rf.last_snapShot_commit_ = index
	//store kv
	rf.snapshot = snapshot
	rf.persist()
	for i, _ := range rf.nextIndex {
		rf.nextIndex[i] = rf.indexAdd(len(rf.log))
		rf.matchIndex[i] = rf.last_snapShot_commit_
	}
	rf.matchIndex[rf.me] = rf.indexAdd(len(rf.log) - 1)
	// DPrintfln("[C]", ColorBlue, "name:%d snapshot index:%d , loglen:%d, last_snapShot_commit_:%d\n", rf.me, index, len(rf.log), rf.last_snapShot_commit_)

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
		expire := 150 + (rand.Int63() % 300)
		//fmt.Printf(" %v \n", expire)
		if rf.state != Candidate {
			rf.lock_.Unlock()
			return
		}
		rf.term++
		// DPrintfln("[G]", ColorGreen, "term:%d have chance to be leader name%d\n", rf.term, rf.me)

		n := 1
		rf.votedFor = rf.me
		rf.persist()
		var lastIndex int
		var lastTerm int
		if len(rf.log) > 1 {
			lastIndex = rf.indexAdd(len(rf.log) - 1)
			lastTerm = rf.log[len(rf.log)-1].Term
		} else {
			lastIndex = rf.last_snapShot_commit_
			lastTerm = rf.last_snapShot_term_
		}
		args := RequestVoteArgs{
			Term:         rf.term,
			CandidateId:  rf.me,
			LastLogIndex: lastIndex,
			LastLogTerm:  lastTerm,
		}
		for i, _ := range rf.peers {
			if rf.me == i {
				continue
			}
			reply := RequestVoteReply{}
			go rf.sendRequestVote(i, &args, &reply)

		}
		rf.lock_.Unlock()

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
						//DPrintfln("[C]", ColorRed, "name:%d args term:%d   rf.term:%d    become follower%v\n", rf.me, args.Term, rf.term)
						rf.persist()
						rf.lock_.Unlock()
						return
					}
					rf.lock_.Unlock()
					continue
				}
				if reply.VoteGranted {
					n++
					// DPrintfln("[C]", ColorBlue, " term:%d get vote num :%d    myname:%d . n", rf.term, n, rf.me)
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

// tttag
// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	// DPrintfln("[d]", ColorYellow, "name:%d  receive vote request from name:%d  term :%d\n", rf.me, args.CandidateId, args.Term)

	rf.lock_.Lock()
	defer rf.lock_.Unlock()

	rf.time_ = time.Now()

	flag := false
	if args.Term > rf.term {
		rf.state = Follower
		rf.term = args.Term
		rf.votedFor = -1
		flag = true
		// rf.persist()
	}

	reply.Term = rf.term
	if args.Term < rf.term {
		reply.VoteGranted = false
		return
	}
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.VoteGranted = false
		return
	}
	var lastIndex int
	var lastTerm int
	if len(rf.log) > 1 {
		lastIndex = rf.indexAdd(len(rf.log) - 1)
		lastTerm = rf.log[len(rf.log)-1].Term
	} else {
		lastIndex = rf.last_snapShot_commit_
		lastTerm = rf.last_snapShot_term_
	}

	if args.LastLogTerm < lastTerm || (args.LastLogTerm == lastTerm && args.LastLogIndex < lastIndex) {
		reply.VoteGranted = false
		if flag {
			rf.persist()
		}
		return
	}
	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
	rf.persist()
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
	rf.lock_.Lock()

	isLeader = rf.state == Leader
	if isLeader {
		rf.log = append(rf.log, Log{Term: rf.term, Cmd: command})
		// DPrintfln("[C]", ColorGreen, "insert log term:%d    name:%d   isLeader:%v index:%d.  cmd:%v \n", rf.term, rf.me, isLeader, rf.last_snapShot_commit_+len(rf.log)-1, command)
		// DPrintfln("[C]", ColorGreen, "insert log term:%d    name:%d   isLeader:%v .  cmd \n", rf.term, rf.me, isLeader)
		last := rf.indexAdd(len(rf.log)) - 1
		// rf.matchIndex[rf.me] = last
		rf.nextIndex[rf.me] = last + 1

		rf.matchIndex[rf.me] = last
		rf.persist()
	}

	index = rf.indexAdd(len(rf.log) - 1)
	term = rf.term

	rf.lock_.Unlock()

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
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// rf.lock_.Lock()

		// rf.lock_.Unlock()
		ms := 150 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		rf.lock_.Lock()
		if time.Since(rf.time_) > time.Duration(ms)*time.Millisecond && (rf.state == Follower || rf.votedFor == -1) {

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
		term:                  0,
		votedFor:              -1,
		log:                   make([]Log, 1),
		commitIndex:           0,
		lastApplied:           0,
		nextIndex:             make([]int, len(peers)),
		matchIndex:            make([]int, len(peers)),
		time_:                 time.Now(),
		last_snapShot_commit_: 0,
		C_:                    make(chan RequestVoteReply),
		state:                 Follower,
		lock_:                 sync.Mutex{},
	}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	rf.cond = sync.NewCond(&rf.lock_)
	// Your initialization code here (3A, 3B, 3C).
	// DPrintfln("[C]", ColorBlue, "   Make a new Raft%d\n", rf.me)
	//go rf.RequestVoteHelper()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.log[0] = Log{
		Term: rf.last_snapShot_term_,
		Cmd:  "",
	}
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.ReachToCommit()

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
	Term     int
	Success  bool
	XTerm    int
	XIndex   int
	XLen     int
	SnapShot bool
}

func (rf *Raft) startLearder() {

	// DPrintfln("[C]", ColorRed, "   start a Leader state: %d   name:%d   term :%d\n", rf.state, rf.me, rf.term)
	rf.lock_.Lock()
	for i, _ := range rf.nextIndex {
		rf.nextIndex[i] = rf.indexAdd(len(rf.log))
		rf.matchIndex[i] = rf.last_snapShot_commit_
	}
	rf.matchIndex[rf.me] = rf.indexAdd(len(rf.log)) - 1
	rf.lock_.Unlock()
	go rf.pushToFollowers()
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			// DPrintfln("[C]", ColorRed, "  leader create loop myname:%d name:%d   term :%d\n", rf.me, i, rf.term)

			for {
				rf.lock_.Lock()
				if rf.state != Leader || rf.killed() {
					// DPrintfln("[C]", ColorRed, "  leader killed loop myname:%d name:%d   term :%d\n", rf.me, i, rf.term)
					rf.lock_.Unlock()
					return
				}

				if rf.nextIndex[i] <= rf.last_snapShot_commit_ {
					// DPrintfln("[W]", ColorRed, "name:%d to:%d  precommit:%d send installSnapshot ok next:%d", rf.me, i, rf.last_snapShot_commit_, rf.nextIndex[i])
					go func(i int) {
						rf.lock_.Lock()
						args := InstallSnapshotArgs{
							Term:              rf.term,
							LeaderId:          rf.me,
							LastIncludedIndex: rf.last_snapShot_commit_,
							LastIncludedTerm:  rf.last_snapShot_term_,
							Offset:            0,
							Data:              rf.snapshot,
							Done:              true,
						}
						reply := InstallSnapshotReply{}
						rf.lock_.Unlock()
						ok := rf.SendInstallSnapshot(i, &args, &reply)
						rf.lock_.Lock()
						if ok {
							if reply.Term > rf.term {

								rf.term = reply.Term
								rf.state = Follower
								rf.votedFor = -1
								rf.persist()
								rf.lock_.Unlock()
								return
							}

							rf.nextIndex[i] = rf.last_snapShot_commit_ + 1
							rf.matchIndex[i] = rf.last_snapShot_commit_
						}
						rf.lock_.Unlock()
					}(i)
				} else {
					go func(i int) {
						rf.lock_.Lock()
						// DPrintfln("[W]", ColorYellow, "name:%d to:%d  precommit:%d send AppendEntriesArgs ok next:%d", rf.me, i, rf.last_snapShot_commit_, rf.nextIndex[i])
						args := AppendEntriesArgs{
							Term:         rf.term,
							LeaderId:     rf.me,
							PrevLogIndex: rf.nextIndex[i] - 1,
							PrevLogTerm:  rf.log[rf.indexSub(rf.nextIndex[i]-1)].Term,
							// Entries:      rf.log[rf.nextIndex[i]:],
							LeaderCommit: rf.commitIndex,
						}
						if rf.indexAdd(len(rf.log)) <= rf.nextIndex[i] {
							args.Entries = []Log{}

						} else {
							entries := make([]Log, len(rf.log[rf.indexSub(rf.nextIndex[i]):]))
							copy(entries, rf.log[rf.indexSub(rf.nextIndex[i]):])
							args.Entries = entries
						}
						myNext := rf.indexAdd(len(rf.log))
						rf.lock_.Unlock()
						// DPrintfln("[W]", ColorYellow, "send AppendEntriesArgs ok next:%d", rf.nextIndex[i])
						reply := AppendEntriesReply{}
						ok := rf.SendAppendEntries(i, &args, &reply)
						rf.lock_.Lock()
						if ok {
							if reply.Term > rf.term {

								rf.term = reply.Term
								rf.state = Follower
								rf.votedFor = -1
								rf.persist()
								rf.lock_.Unlock()
								return
							}
							if !reply.Success {
								switch {
								case reply.XLen != -1:
									rf.nextIndex[i] = minInt(reply.XLen, rf.indexAdd(len(rf.log)))
								case reply.XTerm != -1:
									pos := rf.BinarySearch(reply.XTerm)

									if pos < 0 {
										rf.nextIndex[i] = minInt(reply.XIndex, rf.indexAdd(len(rf.log)))
									} else {
										rf.nextIndex[i] = minInt(rf.indexAdd(pos), rf.indexAdd(len(rf.log)))
									}
								default:
									rf.nextIndex[i] = maxInt(1, rf.nextIndex[i]-1)
								}
								// if rf.nextIndex[i] <= 0 {
								// 	fmt.Printf("%v \n. %v\n", args, reply)
								// }
							} else {
								rf.nextIndex[i] = myNext
								rf.matchIndex[i] = rf.nextIndex[i] - 1
							}
						}
						rf.lock_.Unlock()
					}(i)
				}
				rf.lock_.Unlock()
				time.Sleep(50 * time.Millisecond)
			}
		}(i)
	}

}

func (rf *Raft) SendAppendEntries(i int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {

	ok := rf.peers[i].Call("Raft.AppendEntries", args, reply)
	return ok
}
func (rf *Raft) BinarySearch(Xterm int) int {
	l, r := 0, len(rf.log)
	for l < r {
		mid := l + (r-l)/2
		if rf.log[mid].Term <= Xterm {
			l = mid + 1
		} else {
			r = mid
		}
	}

	if l > 0 && rf.log[l-1].Term == Xterm {
		return l
	}
	return -1
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// DPrintfln("[C]", ColorBlue, "   name:%d from:%d receive .  ppendEntries \n", rf.me, args.LeaderId)
	rf.lock_.Lock()
	reply.XIndex = -1
	reply.XLen = -1
	reply.XTerm = -1
	if args.Term < rf.term {
		reply.Success = false
		reply.Term = rf.term
		rf.lock_.Unlock()
		return
	}
	rf.time_ = time.Now()

	if args.Term > rf.term {
		rf.term = args.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()
	}

	log_index := len(rf.log) - 1

	PrevLogIndex := rf.indexSub(args.PrevLogIndex)

	if log_index < PrevLogIndex || rf.log[PrevLogIndex].Term != args.PrevLogTerm {
		// DPrintfln("[W]", ColorRed, "receive log not matched myname:%d mylog_index:%d mylog_last_term:%d, receive_name:%d, receive_index:%d  , receive_last_term:%d\n", rf.me, log_index, rf.log[args.PrevLogIndex].Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm)

		reply.Success = false
		reply.Term = rf.term
		if len(rf.log) <= PrevLogIndex {
			reply.XLen = rf.indexAdd(len(rf.log))
		} else {
			reply.XTerm = rf.log[PrevLogIndex].Term
			index := PrevLogIndex
			for index > 0 && rf.log[index-1].Term == reply.XTerm {
				index--
			}
			reply.XIndex = rf.indexAdd(index)
		}

		rf.lock_.Unlock()
		return
	}

	if len(args.Entries) == 0 {

		reply.Term = rf.term

		reply.Success = true

	} else {

		// DPrintfln("[B]", ColorGreen, "receive log matched myname:%d mylog_index:%d mylog_last_term:%d, receive_name:%d, receive_index:%d  , receive_last_term:%d\n", rf.me, log_index, rf.log[args.PrevLogIndex].Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm)
		index := PrevLogIndex + 1
		for i, value := range args.Entries {
			if log_index < index+i {
				rf.log = append(rf.log, value)
			} else if rf.log[index+i].Term != args.Entries[i].Term {
				rf.log = rf.log[:index+i]
				rf.log = append(rf.log, value)
			}
			log_index = len(rf.log) - 1
			// DPrintfln("[B]", ColorGreen, "receive log matched myname:%d mylog_index:%d mylog_last_term:%d, receive_name:%d ,cmd:%v\n", rf.me, log_index, rf.log[args.PrevLogIndex].Term, args.LeaderId, rf.log[log_index].Cmd)

		}
		//DPrintfln("[b]", ColorGreen, "receive log matched myname:%d myloglen%d\n", rf.me, len(rf.log))
		rf.persist()

		reply.Success = true

	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = minInt(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
		rf.cond.Signal()
	}
	rf.lock_.Unlock()

}

func (rf *Raft) pushToFollowers() {

	go func() {
		for {
			time.Sleep(50 * time.Millisecond)
			rf.lock_.Lock()
			if len(rf.log)-1 <= rf.indexSub(rf.commitIndex) {
				rf.lock_.Unlock()
				continue
			}
			if rf.killed() || rf.state != Leader {
				rf.lock_.Unlock()
				return
			}
			for N := len(rf.log) - 1; N > rf.indexSub(rf.commitIndex); N-- {
				n := 0

				for _, value := range rf.matchIndex {
					if value >= rf.indexAdd(N) {
						n++
					}
				}
				//DPrintfln("[C]", ColorGreen, "want to update name:%d  index:%d   receive_commit_num:%d   peers_num:%d  rf.log[N].Term:%d == rf.term:%d  \n", rf.me, N, n, len(rf.peers), rf.log[N].Term, rf.term)
				if n >= len(rf.peers)/2+1 && rf.log[N].Term == rf.term {
					rf.commitIndex = rf.indexAdd(N)
					rf.cond.Signal()
					DPrintfln("[P]", ColorGreen, "index update success :%d    commitIndex:%d \n", rf.me, rf.commitIndex)

					break
				}
			}
			rf.lock_.Unlock()
		}

	}()

}

func (rf *Raft) ReachToCommit() {
	for {
		rf.lock_.Lock()
		for rf.lastApplied >= rf.commitIndex && !rf.killed() {
			rf.cond.Wait()
		}
		if rf.killed() {
			rf.lock_.Unlock()
			return
		}
		start := rf.lastApplied + 1
		end := rf.commitIndex
		entries := rf.log[rf.indexSub(start) : rf.indexSub(end)+1]
		rf.lock_.Unlock()

		for i, entry := range entries {
			index := start + i
			msg := raftapi.ApplyMsg{
				CommandValid: true,
				Command:      entry.Cmd,
				CommandIndex: index,
			}
			// DPrintfln("[W]", ColorYellow, "push success name:%d    index:%d   cmd:%v\n", rf.me, index, entry.Cmd)
			rf.applyCh <- msg
			rf.lock_.Lock()
			rf.lastApplied = index
			rf.lock_.Unlock()
		}
	}
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}
type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) SendInstallSnapshot(i int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {

	ok := rf.peers[i].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.lock_.Lock()
	defer rf.lock_.Unlock()
	reply.Term = rf.term
	if args.Term < rf.term {
		return
	}
	if args.Term > rf.term {
		rf.term, rf.votedFor, rf.state = args.Term, -1, Follower
		rf.persist()
	}
	// DPrintfln("[W]", ColorYellow, "install snap receive name:%d lastcommit:%d   len:%d   \n", rf.me, rf.last_snapShot_commit_, len(rf.log))
	rf.time_ = time.Now()
	if args.LastIncludedIndex <= rf.last_snapShot_commit_ {
		return
	}

	if args.Offset == 0 {
		//create a new snapshot
		rf.snapshot = make([]byte, 0)
	}
	rf.snapshot = append(rf.snapshot, args.Data...)
	if !args.Done {
		return
	}
	// DPrintfln("[W]", ColorYellow, ":%d 314194831748738   index:%d   \n", rf.me, rf.lastApplied)
	index := rf.indexSub(args.LastIncludedIndex)

	if index < len(rf.log) && rf.log[index].Term == args.LastIncludedTerm {
		rf.log = rf.log[index:]
	} else {
		rf.log = []Log{{Term: args.LastIncludedTerm, Cmd: ""}}
	}

	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
		rf.lastApplied = args.LastIncludedIndex
	}
	rf.last_snapShot_commit_ = args.LastIncludedIndex
	rf.last_snapShot_term_ = args.LastIncludedTerm
	rf.persist()
	//Save snapshot file, discard any existing or partial snapshot with a smaller index
	go func() {
		rf.applyCh <- raftapi.ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}()

}
func (rf *Raft) indexSub(index int) int {
	return index - rf.last_snapShot_commit_
}
func (rf *Raft) indexAdd(index int) int {
	return index + rf.last_snapShot_commit_
}

func (rf *Raft) indexSubForIndex(index int) int {
	return index - rf.last_snapShot_commit_
}
func (rf *Raft) indexAddForIndex(index int) int {
	return index + rf.last_snapShot_commit_
}
