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
const (
	dEelect string = "ELECT"
	dRVote  string = "REQUESTVOTE"
	dVote   string = "VOTE"
	dLeader string = "LEADER"
	dGLog   string = "GETLOG"
	dCLog   string = "COPYLOG"
	dCMLOG  string = "COMMITLOG"
	dHeart  string = "HEARTBEATS"
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
	mu                       sync.Mutex // Lock to protect shared access to this peer's state
	rpcLock                  []sync.Mutex
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
	log                      []Entry
	debug                    Debuger
	commitSignal             chan int
	appendLogFlag            []bool
	isConnect                []bool
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
	if args.Term >= rf.currentTerm {
		rf.debug.Debug(" 收到 S%d 投票请求", rf.debug.VoteFlag, dVote, args.CandidateId)
		if args.Term > rf.currentTerm {
			rf.mu.Lock()
			rf.state = follower
			rf.currentTerm = args.Term
			reply.Term = rf.currentTerm
			rf.voteFor = -1
			rf.mu.Unlock()
		}
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
		if rf.voteFor == -1 || rf.voteFor == args.CandidateId {
			rf.debug.Debug(" 向 -> S%d 投票", rf.debug.VoteFlag, dVote, args.CandidateId)
			rf.restartElectionTimerFlag = true
			reply.VoteGranted = true
			rf.voteFor = args.CandidateId
		}
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
	Xitem   int
	Xindex  int
	Upnext  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	defer func() {
		rf.persist()
	}()
	reply.Upnext = args.PrevLogIndex + 1
	reply.Xitem = -1
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
	rf.voteFor = -1
	rf.mu.Unlock()
	if rf.lastLogIndex < args.PrevLogIndex {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.Upnext = rf.lastLogIndex + 1
		reply.Xitem = -1
		rf.debug.Debug(" <- S%d 收到心跳,Upnext:%d", rf.debug.HeartFlag, dHeart, args.LeaderId, reply.Upnext)
		return
	}
	if rf.log[args.PrevLogIndex].Item != args.PrevLogItem {
		reply.Xitem = rf.log[args.PrevLogIndex].Item
		for i := args.PrevLogIndex; i >= 0; i-- {
			if rf.log[i].Item != reply.Xitem {
				reply.Xindex = i + 1
				break
			}
		}
		rf.lastLogIndex = args.PrevLogIndex - 1
		rf.lastLogItem = rf.log[args.PrevLogIndex-1].Item
		reply.Success = false
		reply.Term = rf.currentTerm
		rf.debug.Debug(" <- S%d 收到心跳,Xi:%d,Xt%d", rf.debug.HeartFlag, dHeart, args.LeaderId, reply.Xindex, reply.Xitem)
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
		rf.lastLogIndex = args.Entries[EntriesLen-1].Index
		rf.lastLogItem = args.Entries[EntriesLen-1].Item
		rf.debug.Debug(" <- S%d 日志复制成功 I:%d-I:%d", rf.debug.LogFlag, dCLog, args.LeaderId, args.Entries[0].Index, args.Entries[EntriesLen-1].Index)
	} else {
		rf.lastLogIndex = args.PrevLogIndex
		rf.lastLogItem = args.PrevLogItem
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
			rf.debug.Debug(" 提交日志 I:%d T:%d C:%v", rf.debug.LogFlag, dCMLOG, rf.commitIndex, rf.log[rf.commitIndex].Item, rf.log[rf.commitIndex].Command)
		}
	}
	reply.Upnext = rf.lastLogIndex + 1
	reply.Success = true
	reply.Term = rf.currentTerm
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
	rf.debug.Debug(" 收到日志，I:%d T:%d C:%v", rf.debug.LogFlag, dGLog, index, term, command)
	rf.mu.Unlock()
	// rf.appendLogSignal <- 1
	return index, term, isLeader
}

func (rf *Raft) AppendLog(i int) {
	if rf.state != leader {
		return
	}
	AppendLogArgs := AppendEntriesArgs{}
	AppendLogReply := AppendEntriesReply{}
	rf.mu.Lock()
	AppendLogArgs.LeaderId = rf.me
	AppendLogArgs.LeaderCommit = rf.commitIndex
	AppendLogArgs.Term = rf.currentTerm
	entriesLen := 0
	AppendLogArgs.PrevLogIndex = rf.nextIndex[i] - 1
	AppendLogArgs.PrevLogItem = rf.log[rf.nextIndex[i]-1].Item
	rf.mu.Unlock()
	if rf.appendLogFlag[i] {
		entriesLen = rf.lastLogIndex - rf.nextIndex[i] + 1
	}
	entries := make([]Entry, entriesLen)
	for j := 0; j < entriesLen; j++ {
		entries[j] = rf.log[rf.nextIndex[i]+j]
	}
	AppendLogArgs.Entries = entries
	if entriesLen > 0 {
		rf.debug.Debug(" -> S%d 复制日志 I:%d-I:%d", rf.debug.LogFlag, dCLog, i, entries[0].Index, entries[entriesLen-1].Index)
	}
	ok := rf.sendAppendEntries(i, &AppendLogArgs, &AppendLogReply)
	if ok {
		if AppendLogReply.Success {
			rf.debug.Debug(" S%d 一致性检验通过,prei%d,pret%d", rf.debug.HeartFlag, dHeart, i, AppendLogArgs.PrevLogIndex, AppendLogArgs.PrevLogItem)
			rf.mu.Lock()
			rf.matchIndex[i] = AppendLogArgs.PrevLogIndex + entriesLen
			rf.nextIndex[i] = rf.matchIndex[i] + 1
			rf.appendLogFlag[i] = true
			rf.mu.Unlock()
			return
		} else {
			rf.appendLogFlag[i] = false
			rf.debug.Debug(" S%d 日志不一致,upnext:%d,xi:%d,xt:%d", rf.debug.HeartFlag, dHeart, i, AppendLogReply.Upnext, AppendLogReply.Xindex, AppendLogReply.Xitem)
			if AppendLogReply.Xitem == -1 {
				rf.mu.Lock()
				rf.nextIndex[i] = AppendLogReply.Upnext
				rf.mu.Unlock()
			} else {
				if rf.log[AppendLogReply.Xindex].Item != AppendLogReply.Xitem {
					rf.mu.Lock()
					rf.nextIndex[i] = AppendLogReply.Xindex
					rf.mu.Unlock()
				} else {
					for j := AppendLogReply.Xindex; j < len(rf.log); j++ {
						if rf.log[j].Item != AppendLogReply.Xitem {
							rf.mu.Lock()
							rf.nextIndex[i] = j
							rf.mu.Unlock()
							break
						}
					}
				}
			}
		}
	}

}
func (rf *Raft) Heartsbeat(i int) {
	if rf.state != leader {
		return
	}
	AppendLogArgs := AppendEntriesArgs{}
	AppendLogReply := AppendEntriesReply{}
	rf.mu.Lock()
	AppendLogArgs.LeaderId = rf.me
	AppendLogArgs.LeaderCommit = rf.commitIndex
	AppendLogArgs.Term = rf.currentTerm
	AppendLogArgs.PrevLogIndex = len(rf.log) - 1
	AppendLogArgs.PrevLogItem = rf.log[len(rf.log)-1].Item
	rf.mu.Unlock()
	entries := make([]Entry, 0)
	AppendLogArgs.Entries = entries
	rf.debug.Debug(" ->S%d 发送心跳", rf.debug.HeartFlag, dHeart, i)
	ok := rf.sendAppendEntries(i, &AppendLogArgs, &AppendLogReply)
	if ok {
		rf.isConnect[i] = true
	} else {
		rf.isConnect[i] = false
	}
	// if ok {
	// 	if AppendLogReply.Success {
	// 		rf.debug.Debug(" S%d 一致性检验通过,prei%d,pret%d", rf.debug.HeartFlag, dHeart, i, AppendLogArgs.PrevLogIndex, AppendLogArgs.PrevLogItem)
	// 		if rf.appendLogFlag[i] {
	// 			if rf.lastLogIndex > rf.matchIndex[i] {
	// 				rf.appendLogFlag[i] = false
	// 				rf.AppendLog(i)
	// 				rf.appendLogFlag[i] = true
	// 			}
	// 		}
	// 		return
	// 	} else {
	// 		rf.debug.Debug(" S%d 日志不一致,upnext:%d,xi:%d,xt:%d", rf.debug.HeartFlag, dHeart, i, AppendLogReply.Upnext, AppendLogReply.Xindex, AppendLogReply.Xitem)
	// 		if AppendLogReply.Xitem == -1 {
	// 			rf.mu.Lock()
	// 			rf.nextIndex[i] = AppendLogReply.Upnext
	// 			rf.mu.Unlock()
	// 		} else {
	// 			if rf.log[AppendLogReply.Xindex].Item != AppendLogReply.Xitem {
	// 				rf.mu.Lock()
	// 				rf.nextIndex[i] = AppendLogReply.Xindex
	// 				rf.mu.Unlock()
	// 			} else {
	// 				for j := AppendLogReply.Xindex; j < len(rf.log); j++ {
	// 					if rf.log[j].Item != AppendLogReply.Xitem {
	// 						rf.mu.Lock()
	// 						rf.nextIndex[i] = j
	// 						rf.mu.Unlock()
	// 						break
	// 					}
	// 				}
	// 			}
	// 		}
	// 	}
	// }
}

func (rf *Raft) CheckLog(i int) {
	for rf.state == leader && !rf.killed() {
		if rf.lastLogIndex >= rf.nextIndex[i] && rf.isConnect[i] {
			rf.AppendLog(i)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
func (rf *Raft) CommitLog() {
	for rf.state == leader && !rf.killed() {
		matchIndex := make([]int, rf.severNum)
		for i := 0; i < rf.severNum; i++ {
			matchIndex[i] = rf.matchIndex[i]
		}
		sort.Ints(matchIndex)
		commitIndex := matchIndex[rf.severNum/2]
		for rf.commitIndex < commitIndex && commitIndex > 0 && rf.log[commitIndex].Item == rf.currentTerm {
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
			rf.debug.Debug(" 提交日志 I:%d T:%d C:%v", rf.debug.LogFlag, dCMLOG, rf.commitIndex, rf.log[rf.commitIndex].Item, rf.log[rf.commitIndex].Command)
		}
		time.Sleep(10 * time.Millisecond)
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
				if rf.voteFor == -1 {
					rf.state = candidate
				}
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
	for i := 0; i < rf.severNum; i++ {
		if i != rf.me {
			go rf.Heartsbeat(i)
		}
	}
}
func (rf *Raft) Election() {
	rf.debug.Debug(" 选举T:%d,LT:%d,LI%d", rf.debug.ElectFlag, dEelect, rf.currentTerm, len(rf.log)-1, rf.log[len(rf.log)-1].Item)
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
			LastLogIndex: len(rf.log) - 1,
			LastLogItem:  rf.log[len(rf.log)-1].Item}
		reqReply[i] = RequestVoteReply{}
		if i != rf.me {
			go func(i int) {
				rf.debug.Debug(" -> S%d 请求投票", rf.debug.VoteFlag, dRVote, i)
				ok := rf.sendRequestVote(i, &reqArgs[i], &reqReply[i])
				if ok {
					if reqReply[i].VoteGranted {
						rf.mu.Lock()
						rf.voted[i] = true
						rf.mu.Unlock()
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
			if rf.state == follower {
				return
			}
		}
	}
}
func (rf *Raft) LeaderInit() {
	rf.debug.Debug(" 成为领导者", rf.debug.LeaderFlag, dLeader)
	rf.nextIndex = make([]int, rf.severNum)
	rf.matchIndex = make([]int, rf.severNum)
	for i := 0; i < rf.severNum; i++ {
		rf.nextIndex[i] = rf.lastLogIndex + 1
		rf.matchIndex[i] = 0
		rf.appendLogFlag[i] = true
		rf.isConnect[i] = true
		if i != rf.me {
			go rf.CheckLog(i)
		}
	}
	go rf.CommitLog()
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
	rf.rpcLock = make([]sync.Mutex, rf.severNum)
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
	rf.commitSignal = make(chan int, 10)
	rf.appendLogFlag = make([]bool, rf.severNum)
	rf.isConnect = make([]bool, rf.severNum)
	rf.debug = Debuger{}
	rf.debug.InitDebuger(rf.me, &rf.currentTerm)
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.lastLogIndex = len(rf.log) - 1
	rf.lastLogItem = rf.log[rf.lastLogIndex].Item
	// start ticker goroutine to start elections

	go rf.ticker()

	return rf
}
