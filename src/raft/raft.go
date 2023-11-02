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
	"bytes"
	"sort"

	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
const (
	candidate int = 3
	follower  int = 2
	leader    int = 1
)

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
type Entry struct {
	Command interface{}
	Item    int
	Index   int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu                       sync.Mutex          // Lock to protect shared access to this peer's state
	peers                    []*labrpc.ClientEnd // RPC end points of all peers
	persister                *Persister          // Object to hold this peer's persisted state
	me                       int                 // this peer's index into peers[]
	dead                     int32               // set by Kill()
	applyMsg                 chan ApplyMsg
	state                    int
	currentTerm              int
	lastLogIndex             int
	lastLogItem              int
	severNum                 int
	voteFor                  int
	electionTimeOut          bool
	restartElectionTimerFlag bool
	voted                    []bool
	commitIndex              int
	nextIndex                []int
	matchIndex               []int
	isConnected              []bool
	log                      []Entry
	AppendLock               []bool
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = false
	if rf.state == leader {
		isleader = true
	}
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.voteFor)
	e.Encode(rf.currentTerm)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var voteFor int
	var currentTerm int
	var log []Entry
	if d.Decode(&voteFor) != nil ||
		d.Decode(&currentTerm) != nil ||
		d.Decode(&log) != nil {

	} else {
		rf.voteFor = voteFor
		rf.currentTerm = currentTerm
		rf.log = log
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
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
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogItem  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	defer func() {
		rf.persist()
	}()
	// Your code here (2A, 2B).
	// fmt.Printf("节点%d向%d请求投票\n", args.CandidateId, rf.me)
	// fmt.Printf("节点%d向%d发起请求,候选者节点的lastItem为%d，lastIndex为%d,节点的lastItem为%d，lastIndex为%d\n", args.CandidateId, rf.me, args.LastLogItem, args.LastLogIndex, rf.lastLogItem, rf.lastLogIndex)
	if args.Term > rf.currentTerm {
		rf.mu.Lock()
		rf.state = follower
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		if args.LastLogItem < rf.lastLogItem {
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
			return
		}
		if args.LastLogItem == rf.lastLogItem && args.LastLogIndex < rf.lastLogIndex {
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
			return
		}
		// fmt.Printf("节点%d投票给%d,候选者节点的lastItem为%d，lastIndex为%d\n", rf.me, args.CandidateId, args.LastLogItem, args.LastLogIndex)
		reply.VoteGranted = true
		rf.voteFor = args.CandidateId
	} else {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogItem  int
	LeaderCommit int
	Entries      []Entry
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	defer func() {
		rf.persist()
	}()
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	rf.mu.Lock()
	rf.state = follower
	rf.restartElectionTimerFlag = true
	rf.currentTerm = args.Term
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
	if rf.lastLogIndex < args.PrevLogIndex {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	if rf.log[args.PrevLogIndex].Item != args.PrevLogItem {
		rf.lastLogIndex = args.PrevLogIndex - 1
		if rf.lastLogItem > rf.log[args.PrevLogIndex-1].Item {
			fmt.Printf("append error1\n")
		}
		rf.lastLogItem = rf.log[args.PrevLogIndex-1].Item
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	for i := 0; i < len(args.Entries); i++ {
		if args.Entries[i].Index == len(rf.log) {
			rf.log = append(rf.log, args.Entries[i])
		} else {
			rf.log[args.Entries[i].Index] = args.Entries[i]
		}
	}
	EntriesLen := len(args.Entries)
	if EntriesLen > 0 {
		if rf.lastLogItem > args.Entries[EntriesLen-1].Item {
			fmt.Printf("append error2\n")
		}
		rf.lastLogIndex = args.Entries[EntriesLen-1].Index
		rf.lastLogItem = args.Entries[EntriesLen-1].Item
	}
	if len(rf.log) >= 3 {
		if rf.log[1].Command == rf.log[2].Command {
			fmt.Println("append error")
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		for rf.commitIndex < rf.lastLogIndex && rf.commitIndex < args.LeaderCommit {
			rf.commitIndex++
			rf.applyMsg <- ApplyMsg{
				CommandValid:  true,
				Command:       rf.log[rf.commitIndex].Command,
				CommandIndex:  rf.commitIndex,
				SnapshotValid: false,
				Snapshot:      []byte{},
				SnapshotTerm:  0,
				SnapshotIndex: 0,
			}
		}
	}
	reply.Success = true
	reply.Term = rf.currentTerm
	// fmt.Printf("节点%d成功复制日志%d,日志内容是%d\n", rf.me, rf.lastLogIndex, args.Entries.Command)
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
	rf.persist()
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.persist()
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	index := rf.lastLogIndex + 1
	term := rf.currentTerm
	isLeader := false
	if rf.state == leader {
		isLeader = true
	} else {
		return index, term, isLeader
	}
	// Your code here (2B).
	rf.mu.Lock()
	rf.lastLogIndex += 1
	rf.lastLogItem = rf.currentTerm
	rf.matchIndex[rf.me] = rf.lastLogIndex
	rf.nextIndex[rf.me] = rf.lastLogIndex + 1
	entry := Entry{
		Command: command,
		Item:    rf.currentTerm,
		Index:   rf.lastLogIndex,
	}
	rf.log = append(rf.log, entry)
	index = entry.Index
	term = entry.Item
	// fmt.Printf("节点%d添加日志%v\n", rf.me, entry)
	rf.mu.Unlock()
	return index, term, isLeader
}

func (rf *Raft) AppendLog(i int) {
	AppendLogArgs := AppendEntriesArgs{}
	AppendLogReply := AppendEntriesReply{}
	for {
		rf.mu.Lock()
		AppendLogArgs.LeaderId = rf.me
		AppendLogArgs.LeaderCommit = rf.commitIndex
		AppendLogArgs.Term = rf.currentTerm
		entriesLen := rf.lastLogIndex - rf.nextIndex[i] + 1
		AppendLogArgs.PrevLogIndex = rf.nextIndex[i] - 1
		AppendLogArgs.PrevLogItem = rf.log[rf.nextIndex[i]-1].Item
		rf.mu.Unlock()
		if entriesLen == 0 {
			return
		}
		if entriesLen < 0 {
			fmt.Println("Appendlog error")
		}
		entries := make([]Entry, entriesLen)
		for j := 0; j < entriesLen; j++ {
			entries[j] = rf.log[rf.nextIndex[i]+j]
		}
		AppendLogArgs.Entries = entries
		// fmt.Printf("节点%d向节点%d复制日志，日志内容为%v\n", rf.me, i, AppendLogArgs.Entries)
		ok := rf.sendAppendEntries(i, &AppendLogArgs, &AppendLogReply)
		if ok {
			rf.isConnected[i] = true
			if AppendLogReply.Success {
				rf.nextIndex[i] += len(AppendLogArgs.Entries)
				rf.matchIndex[i] = rf.nextIndex[i] - 1
				if rf.nextIndex[i] == rf.lastLogIndex+1 {
					return
				}
			} else {
				if AppendLogReply.Term > rf.currentTerm {
					rf.mu.Lock()
					rf.state = follower
					rf.restartElectionTimerFlag = true
					rf.electionTimeOut = false
					rf.currentTerm = AppendLogReply.Term
					rf.mu.Unlock()
					return
				}
				rf.nextIndex[i]--
			}
		} else {
			rf.isConnected[i] = false
			return
		}
	}
}
func (rf *Raft) CheckLog(i int) {
	for rf.state == leader {
		if rf.lastLogIndex > rf.matchIndex[i] && rf.isConnected[i] {
			rf.AppendLog(i)
		}
		time.Sleep(time.Millisecond)
	}
}
func (rf *Raft) CommitLog() {
	for rf.state == leader {
		matchIndex := make([]int, rf.severNum)
		for i := 0; i < rf.severNum; i++ {
			matchIndex[i] = rf.matchIndex[i]
		}
		sort.Ints(matchIndex)
		commitIndex := matchIndex[rf.severNum/2]
		for rf.commitIndex < commitIndex && commitIndex > 0 {
			rf.commitIndex++
			// fmt.Printf("日志%d以提交\n", rf.commitIndex)
			rf.applyMsg <- ApplyMsg{
				CommandValid:  true,
				Command:       rf.log[rf.commitIndex].Command,
				CommandIndex:  rf.commitIndex,
				SnapshotValid: false,
				Snapshot:      []byte{},
				SnapshotTerm:  0,
				SnapshotIndex: 0,
			}
		}
		time.Sleep(time.Millisecond)
	}
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep()
		if rf.state == leader {
			rf.Heartsbeats()
			rf.HeartsbeatsTimer()
		}
		if rf.state == follower {
			if !rf.electionTimeOut {
				rf.ElectionTimer()
			}
			if rf.electionTimeOut {
				rf.mu.Lock()
				rf.electionTimeOut = false
				rf.state = candidate
				rf.mu.Unlock()
			}
		}
		if rf.state == candidate {
			rf.Election()
		}

	}
}
func (rf *Raft) ElectionTimer() {
	rand.Seed(time.Now().UnixNano() * int64(rf.me))
	s := rand.Intn(150) + 150
	for i := 0; i < s; i++ {
		time.Sleep(time.Millisecond)
		if rf.restartElectionTimerFlag {
			rf.mu.Lock()
			rf.restartElectionTimerFlag = false
			rf.mu.Unlock()
			i = 0
			s = rand.Intn(150) + 150
		}
	}
	rf.electionTimeOut = true
}
func (rf *Raft) HeartsbeatsTimer() {
	time.Sleep(50 * time.Millisecond)
}
func (rf *Raft) Heartsbeats() {
	// ConnectCount := 0
	// for j := 0; j < rf.severNum; j++ {
	// 	if rf.isConnected[j] {
	// 		ConnectCount++
	// 	}
	// }
	// if ConnectCount < (rf.severNum/2 + 1) {
	// 	rf.state = follower
	// 	rf.electionTimeOut = false
	// 	rf.restartElectionTimerFlag = true
	// }
	heatsbeatssArgs := make([]AppendEntriesArgs, rf.severNum)
	heartsbeatsReply := make([]AppendEntriesReply, rf.severNum)
	for i := 0; i < rf.severNum; i++ {
		heatsbeatssArgs[i] = AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.lastLogIndex,
			PrevLogItem:  rf.lastLogItem,
			LeaderCommit: rf.commitIndex,
			Entries:      make([]Entry, 0)}
		if i != rf.me {
			go func(i int) {
				ok := rf.sendAppendEntries(i, &heatsbeatssArgs[i], &heartsbeatsReply[i])
				if ok {
					if heartsbeatsReply[i].Term > rf.currentTerm {
						rf.mu.Lock()
						rf.state = follower
						rf.restartElectionTimerFlag = true
						rf.electionTimeOut = false
						rf.currentTerm = heartsbeatsReply[i].Term
						rf.mu.Unlock()
						return
					}
					rf.isConnected[i] = true
				} else {
					rf.isConnected[i] = false
				}
			}(i)
		}
	}
}
func (rf *Raft) Election() {
	fmt.Printf("节点%d开始选举,任期为%d,在%v\n", rf.me, rf.currentTerm, time.Now())
	rf.currentTerm = rf.currentTerm + 1
	reqArgs := make([]RequestVoteArgs, rf.severNum)
	reqReply := make([]RequestVoteReply, rf.severNum)
	rf.voted = make([]bool, rf.severNum)
	rf.voteFor = rf.me
	voteNum := 1
	for i := 0; i < rf.severNum; i++ {
		rf.voted[i] = false
		reqArgs[i] = RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: rf.lastLogIndex,
			LastLogItem:  rf.lastLogItem}
		reqReply[i] = RequestVoteReply{}
		if i != rf.me {
			go func(i int) {
				ok := rf.sendRequestVote(i, &reqArgs[i], &reqReply[i])
				if ok {
					if reqReply[i].VoteGranted {
						rf.mu.Lock()
						rf.voted[i] = true
						rf.mu.Unlock()
					}
					if reqReply[i].Term > rf.currentTerm {
						rf.state = follower
						rf.restartElectionTimerFlag = true
						rf.electionTimeOut = false
						rf.currentTerm = reqReply[i].Term
					}
				}
			}(i)
		}
	}
	rand.Seed(time.Now().UnixNano() * int64(rf.me))
	s := rand.Intn(150) + 150
	for j := 0; j < s; j++ {
		time.Sleep(time.Millisecond)
		voteNum = 1
		for k := 0; k < rf.severNum; k++ {
			if rf.voted[k] {
				voteNum++
			}
		}
		if voteNum >= (rf.severNum/2 + 1) {
			rf.state = leader
		}
		if rf.state != candidate {
			if rf.state == leader {
				rf.LeaderInit()
				return
			}
		}
	}
	if rf.state == follower {
		return
	}
}
func (rf *Raft) LeaderInit() {
	rf.nextIndex = make([]int, rf.severNum)
	rf.matchIndex = make([]int, rf.severNum)
	for i := 0; i < rf.severNum; i++ {
		rf.nextIndex[i] = rf.lastLogIndex + 1
		rf.matchIndex[i] = 0
		rf.isConnected[i] = true
		rf.AppendLock[i] = false
		if rf.me != i {
			go rf.CheckLog(i)
		}
	}
	go rf.CommitLog()

	fmt.Printf("节点%d成为领导者节点\n", rf.me)
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
	rf.applyMsg = applyCh
	rf.me = me
	rf.currentTerm = 0
	rf.state = follower
	rf.severNum = len(rf.peers)
	rf.voteFor = -1
	rf.electionTimeOut = false
	rf.restartElectionTimerFlag = false
	rf.log = make([]Entry, 0)
	rf.log = append(rf.log, Entry{
		Command: nil,
		Item:    0,
	})
	rf.commitIndex = 0
	rf.lastLogIndex = 0
	rf.lastLogItem = 0
	rf.isConnected = make([]bool, rf.severNum)
	rf.AppendLock = make([]bool, rf.severNum)
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.lastLogIndex = len(rf.log) - 1
	rf.lastLogItem = rf.log[rf.lastLogIndex].Item
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
