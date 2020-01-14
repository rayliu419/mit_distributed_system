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
	"labgob"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "labrpc"

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommitTerm int
}

type Log struct {
	Term    int         // Command executed by term
	Command interface{} // Command
}

type role int

const (
	FOLLOWER = 0 + iota
	CANDIDATE
	LEADER
)

//var programestarttime time.Time

func RoleString(roleint int) string {
	switch roleint {
	case 0:
		return "FOLLOWER"
	case 1:
		return "CANDIDATE"
	case 2:
		return "LEADER"
	default:
		return "invalid role"
	}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// 需要持久化存储的属性，在RPC回复之前，就要持久化到硬盘
	currentterm int   // 当前Raft实例认为的term
	votedfor    int   // 当前term接收到的候选者id, 初始为null。int怎么设置null?
	log         []Log // log

	// 所有机器可变状态
	/*
		将被提交的日志记录的索引(初值为0且单调递增)。由于commitindex没有被持久化，导致一个挂掉的leader回来以后，会重复
		提交command到状态机，但是因为是按顺序提交的，实际也不会有问题？
	 */
	commitindex int
	lastapplied int // 已经被提交到状态机的最后一个日志的索引(初值为0且单调递增)

	// leader可变状态
	/*
		对于某个server来说，leader需要发送给那个server的index，初始化为leader的last log index + 1。数组长度跟peers长度一样。
		不断的试探回退。
	*/
	nextindex []int
	// 每个server log里最高的匹配leader log的index，初始都为0。用来更新commitindex
	matchindex []int
	// 上面这两个数组对于自己那一个元素怎么维护？ - 不会用到

	// 其他未在图二中有的，但是我觉得需要
	role             int
	lastheartbeat    time.Time     // 记录上次的心跳时间
	heartbeattimeout time.Duration // 心跳检测的时间
	electiontimeout  time.Duration // 选举超时时间
	// 用于leader周期性发心跳，在论文中，并没有指明需不需要这个值。只是说在空闲时间发心跳，如果设置为hearttimeout，会有问题。
	heartbeatinteval time.Duration
	leaderid int // 当前的leader

	commitmu sync.Mutex
	commitchan chan ApplyMsg
	commitcond *sync.Cond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentterm
	isleader = false
	if rf.leaderid == rf.me {
		isleader = true
	}
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentterm)
	e.Encode(rf.votedfor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentterm int
	var votefor int
	var log []Log
	if d.Decode(&currentterm) != nil || d.Decode(&votefor) != nil || d.Decode(&log) != nil {
		DPrintf("decode error !")
	} else {
		rf.currentterm = currentterm
		rf.votedfor = votefor
		rf.log = log
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term            int // candidate的term
	CandidateIndex  int // candidate的index
	LastLogIndex    int // candidate的最后一条日志的索引
	LastLogItemTerm int // candidate的最后一条日志的term
	LogId           int // debug
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 这里应该是其他peer当前的term，用于返回给发起投票的candidate更新。
	VoteGranted bool // 是否收到了同意的投票
	LogId       int  // debug
}

// LogNewer这个函数，比较的是log的index/term
func LogNewer(candidatelogindex int, candidatelogterm int, mylogindex int, mylogterm int) bool {
	//DPrintf("candidatelogindex - %v candidatelogterm - %v mylogindex - %v mylogterm - %v",
	//	candidatelogindex, candidatelogterm, mylogindex, mylogterm)
	candidatenewer := false
	if candidatelogterm > mylogterm {
		candidatenewer = true
	} else if candidatelogterm < mylogterm {
		candidatenewer = false
	} else {
		if candidatelogindex > mylogindex {
			candidatenewer = true
		} else if candidatelogindex < mylogindex {
			candidatenewer = false
		} else {
			// 这种情况其实是一样的
			candidatenewer = true
		}
	}
	return candidatenewer
}

// RequestVote处理
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	logid := args.LogId
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//DPrintf("%v : {term - %v votefor - %v}: handle vote from {index - %v term - %v }", rf.me, rf.currentterm, rf.votedfor, args.CandidateIndex, args.Term)
	if args.Term < rf.currentterm {
		DPrintf("[%v] %v : {term - %v role - %v}： refuse vote {index - %v term - %v }\n",
			logid, rf.me, rf.currentterm, RoleString(rf.role), args.CandidateIndex, args.Term)
		reply.VoteGranted = false
	} else {
		if args.Term > rf.currentterm {
			DPrintf("[%v] %v : {term - %v role - %v}: - change term because recevie vote from %v { term - %v }\n",
				logid, rf.me, rf.currentterm, RoleString(rf.role), args.CandidateIndex, args.Term)
			rf.role = FOLLOWER
			rf.currentterm = args.Term
			rf.votedfor = -1
			// 需要重设leaderid
			rf.leaderid = -1
		}
		candidatelogindex := args.LastLogIndex
		candidatelogterm := args.LastLogItemTerm
		mylastlogindex := len(rf.log) - 1
		mylastlogterm := rf.log[mylastlogindex].Term
		// 计算谁的日志更新
		candidatenewer := LogNewer(candidatelogindex, candidatelogterm, mylastlogindex, mylastlogterm)
		voteconditon := false
		if rf.votedfor == -1 || rf.votedfor == args.CandidateIndex {
			// 没有为最新的term投过票或者投过相同的票了？第二个条件是因为可能出现发送两次请求吗？- 包重复
			//DPrintf("%v - %v votecondition is true because votefor is %v\n", args.CandidateIndex, rf.me, rf.votedfor)
			voteconditon = true
		}
		if voteconditon && candidatenewer {
			//DPrintf("accept %v - %v\n", args.CandidateIndex, rf.me)
			reply.VoteGranted = true
			rf.votedfor = args.CandidateIndex
			// 下面这几行在图2中好像没有指明
			rf.role = FOLLOWER
			rf.leaderid = -1
		} else {
			reply.VoteGranted = false
		}
	}
	if reply.VoteGranted {
		DPrintf("[%v] %v : {term - %v role - %v}: accept vote from %v { term - %v }",
			logid, rf.me, rf.currentterm, RoleString(rf.role), args.CandidateIndex, args.Term)
	} else {
		DPrintf("[%v] %v : {term - %v role - %v}: refuse vote from  %v { term - %v }",
			logid, rf.me, rf.currentterm, RoleString(rf.role), args.CandidateIndex, args.Term)
	}
	reply.Term = rf.currentterm
	rf.persist()
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
// 使用labrpc来模拟网络间的访问，实际上使用的是chan模拟。
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
//
/* RPC这个模块看起来work的方式我还有一些没明白
sendRequestVote会阻塞，所以要注意使用goroutine来调用
这里的通信问题是:
1.例如sendRequestVote()发送出去以后，收到的回复可能是long delay，突然收到一个很早前发出去的回复。
2.网络隔离。直接某个node就没了，它发的也出不去，它收也收不到其他node的请求。
3.乱序，什么样的情况？
一般的情况下，每次通信都是同一个端口吗？如果是同一个端口，TCP协议应该保证了是没法乱序的吧？
*/
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	rf.mu.Lock()
	logid := args.LogId
	DPrintf("[%v] %v: sendRequestVote to %v, time - %v, args - %+v\n",
		logid, args.CandidateIndex, server, time.Now(), args)
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	DPrintf("[%v] %v : sendRequestVote receive %v reply, time - %v, args - %+v reply - %+v\n",
		logid, rf.me, server, time.Now(), args, reply)
	if reply.Term > rf.currentterm {
		rf.currentterm = reply.Term
		rf.role = FOLLOWER
		rf.votedfor = -1
		rf.leaderid = -1
		rf.persist()
	}
	rf.mu.Unlock()
	return ok
}

type AppendEntriesArgs struct {
	Term         int // leader的term
	LeaderId     int // leader的index, follower可以设置
	PrevLogIndex int // 这个index代表的含义是：当前复制的entry之前的index
	PrevLogTerm  int // 上面对应的term

	Entries           []Log // 待复制的日志
	LeaderCommitIndex int   // leader的commit index
	LogId             int   // debug
}

type AppendEntriesReply struct {
	Term    int  // 假设follower的term比leader还高，要回复这个term。leader会根据这个值重置自己的term。
	Success bool // 如果follower包含索引为prevLogIndex，且任期为prevLogTerm。
	LogId   int  // debug
	ConflictIndex int
	ConflictTerm int
}


func (rf *Raft) FindFirstIndexInTerm(index int, term int) int {
	DPrintf("FindFirstIndexInTerm : log -%+v index - %v term - %v", rf.log, index, term)
	i := index
	for ; i > 0; i-- {
		if rf.log[i - 1].Term != term {
			return i
		}
	}
	return i
}

// 处理心跳和复制日志的请求
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	logid := args.LogId
	DPrintf("[%v] %v: {term - %v role - %v} receive AppendEntries from %v, args - %+v\n",
		logid, rf.me, rf.currentterm, RoleString(rf.role), args.LeaderId, args)
	if args.Term < rf.currentterm {
		DPrintf("[%v] %v: {term - %v role - %v} : receive invalid heart beat from %v, args - %+v\n",
			logid, rf.me, rf.currentterm, RoleString(rf.role), args.LeaderId, args)
		reply.Success = false
		reply.Term = rf.currentterm
		return
	}
	if args.Term > rf.currentterm {
		DPrintf("[%v] %v : {term - %v role - %v}: - change term because receive heart beat from %v args - %+v\n",
			logid, rf.me, rf.currentterm, RoleString(rf.role), args.LeaderId, args)
		rf.role = FOLLOWER
		rf.currentterm = args.Term
		rf.votedfor = args.LeaderId
		rf.leaderid = args.LeaderId
	}
	if rf.currentterm == args.Term && rf.leaderid != -1 && rf.leaderid != args.LeaderId {
		DPrintf("[%v] %v: {term - %v role - %v} : receive invalid heart beat "+
			"from %v but current term leader is determined to %v, args - %+v\n",
			logid, rf.me, rf.currentterm, RoleString(rf.role), args.LeaderId, rf.leaderid, args)
		reply.Success = false
		reply.Term = rf.currentterm
		reply.ConflictIndex = -1
		return
	}
	/*
		下面的都是合法的appendEntries的请求。
		前面的reply.Success=false并不更新心跳，因为不是来自于合理的leader的情况。
		以下的都需要更新心跳时间
	*/
	DPrintf("[%v] %v: {term - %v role - %v} receive valid heart beat from %v, args - %+v\n",
		logid, rf.me, rf.currentterm, RoleString(rf.role), args.LeaderId, args)
	reply.Success = true
	rf.currentterm = args.Term
	rf.role = FOLLOWER
	rf.votedfor = args.LeaderId
	rf.leaderid = args.LeaderId
	DPrintf("[%v] %v: {term - %v role - %v} update last heart beat because receive from %v, "+
		"%+v\n", logid, rf.me, rf.currentterm, RoleString(rf.role), args.LeaderId, args)
	// 都是合法的，所以要更新心跳时间
	rf.lastheartbeat = time.Now()
	/*
		心跳场景
		1.node1和node2同时发起RequestVote，node1变为leader，node2还在等待vote的回复，接着node1开始发心跳，node2接到
		此心跳，需要更新node2的leaderid。
		2.node1当选为leader后的常规心跳，不用更新node2的leaderid。
		复制日志的场景需要检测日志，并且将reply.Success = false以让leader发更多的同步日志
	*/
	DPrintf("[%v] %v: { term - %v role - %v mylog-%+v mycommitindex - %v remotelog - %+v remotecommitindex - %v }",
		logid, rf.me, rf.currentterm, RoleString(rf.role), rf.log, rf.commitindex, args.Entries, args.LeaderCommitIndex)
	mylastlogindex := len(rf.log) - 1
	if mylastlogindex < args.PrevLogIndex || args.PrevLogTerm != rf.log[args.PrevLogIndex].Term{
		//
		/*
			1.本node没有那么长的log，要leader传更多的日志过来
			2.本node有长于leader的log，但是在PrevLogIndex的位置term不符合。这意味着leader要传更多的日志修复PrevLogIndex的位置日志。
		 */
		DPrintf("[%v] %v: {term - %v role - %v} mylog is too short, ask leader to pass more entries\n",
			logid, rf.me, rf.currentterm, RoleString(rf.role))
		reply.Success = false
		if mylastlogindex < args.PrevLogIndex {
			reply.ConflictIndex = len(rf.log)
		} else {
			reply.ConflictIndex = rf.FindFirstIndexInTerm(args.PrevLogIndex, rf.log[args.PrevLogIndex].Term)
		}
	} else {
		// 传来的日志足够长来纠正本地的日志了。
		// 清除本node从PrevLogIndex到末尾的的日志
		// golang截断操作符，左闭合又开。如果左边==右边，则为空。
		rf.log = rf.log[0 : args.PrevLogIndex + 1]
		// 添加leader发来的其他的日志
		rf.log = append(rf.log, args.Entries...)
		DPrintf("[%v] %v: {term - %v role - %v} after copy logs - %+v\n",
			logid, rf.me, rf.currentterm, RoleString(rf.role), rf.log)
		// 更新本地的commitindex到leader的commitindex
		if rf.commitindex < args.LeaderCommitIndex {
			DPrintf("[%v] %v: {term - %v role - %v} update commit index from %v to %v\n",
				logid, rf.me, rf.currentterm, RoleString(rf.role), rf.commitindex, args.LeaderCommitIndex)
			rf.commitindex = args.LeaderCommitIndex
			DPrintf("[%v] %v: {term - %v role - %v} notify commit logs\n",
				logid, rf.me, rf.currentterm, RoleString(rf.role))
			rf.commitcond.Signal()
		} else {
			DPrintf("[%v] %v: {term - %v role - %v} keep commit index %v\n",
				logid, rf.me, rf.currentterm, RoleString(rf.role), rf.commitindex)
		}
		//for iter := rf.commitindex + 1; iter <= args.LeaderCommitIndex; iter++ {
		//	// 每移动一下本地的commitindex，需要发一条消息
		//	DPrintf("[%v] %v: {term - %v role - %v} commit command %v at index %v\n",
		//		logid, rf.me, rf.currentterm, RoleString(rf.role), rf.log[iter].Command, iter)
		//	rf.commitchan <- ApplyMsg{true, rf.log[iter].Command, iter, rf.log[iter].Term}
		//}
	}
	reply.Term = rf.currentterm
	rf.persist()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.mu.Lock()
	logid := args.LogId
	DPrintf("[%v] %v : sendAppendEntries(send) to %v, time - %v, args - %+v\n",
		logid, rf.me, server, time.Now(), args)
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	DPrintf("[%v] %v : sendAppendEntries(receive) %v reply, time - %v, args - %+v, reply - %+v\n",
		logid, rf.me, server, time.Now(), args, reply)
	if reply.Term > rf.currentterm {
		rf.currentterm = reply.Term
		rf.role = FOLLOWER
		rf.votedfor = -1
		rf.leaderid = -1
		rf.persist()
	}
	rf.mu.Unlock()
	return ok
}

func (rf *Raft) DoVote() {
	for {
		/*
			每过electontimeout的时间检测一下自己是否变为了leader，或者其他人变成了leader。
			自己变为了leader -> rf.role = LEADER
			其他人变成了leader -> rf.role = FOLLOWER
		*/
		rf.mu.Lock()
		if rf.role != CANDIDATE {
			// 不再发起投票
			rf.mu.Unlock()
			break
		}
		// 再发起一轮投票，前一轮如果是long delay也不作数了。
		var voteagree int64 = 1 // 自己先投自己一票
		rf.votedfor = rf.me
		rf.currentterm += 1
		DPrintf("%v : change term from %v to %v, start to vote\n", rf.me, rf.currentterm-1, rf.currentterm)
		term := rf.currentterm
		candidateindex := rf.me
		lastlogindex := len(rf.log) - 1
		lastlogterm := rf.log[lastlogindex].Term
		rf.persist()
		rf.mu.Unlock()
		/*
			注意上面这些参数必须传进来，因为下面的gorountine()调用的时候，对于不同的情况参数可能变化了。
			例如args.Term = rf.currentterm，在向不同的node发送的时候，可能currentterm已经变化了。
		 */
		for i := 0; i < len(rf.peers); i++ {
			// 向各个node发出RequestVote
			if i != rf.me {
				go func(nodeindex int, term int, candidateindex int, lastlogindex int, lastlogterm int) {
					args := &RequestVoteArgs{}
					reply := &RequestVoteReply{}
					args.Term = term
					args.CandidateIndex = candidateindex
					args.LastLogIndex = lastlogindex
					args.LastLogItemTerm = lastlogterm
					logid := rand.Int()
					args.LogId = logid
					reply.LogId = logid
					ok := rf.sendRequestVote(nodeindex, args, reply)
					rf.mu.Lock()
					/*
					 	1.args.Term == rf.currentterm 应该是代表在candidate在投票期间，接收到了其他leader的appendEntries()
					 	发现自己term比较小，更新了自己的currentterm。或者是收到了一个以前的vote的回复，但是上一轮选举已经超时了。
					 	2.rf.role == CANDIDATE 的检查在于，如果已经收到了足够的选票，在语句中修改了rf.role = LEADER，后续收到
					 	yes回复的不再执行变为leader后做的事情。
					*/
					//DPrintf("{ok - %v votegranted - %v role - %v arg.term - %v rf.term - %v }\n", ok, reply.VoteGranted, rf.role, args.Term, rf.currentterm)
					if ok && reply.VoteGranted && rf.role == CANDIDATE && args.Term == rf.currentterm {
						// 如果获取了一个选票
						DPrintf("[%v] %v : get one vote from %v", logid, rf.me, nodeindex)
						atomic.AddInt64(&voteagree, 1)
						// 如果获取了足够的投票转为leader，不足够仅仅voteagree++
						if int(voteagree)*2 > len(rf.peers) {
							DPrintf("[%v] %v : {term - %v } become leader",
								logid, rf.me, rf.currentterm)
							rf.leaderid = rf.me
							rf.role = LEADER
							rf.nextindex = make([]int, len(rf.peers))
							rf.matchindex = make([]int, len(rf.peers))
							lastlogindex := len(rf.log) - 1
							for j := 0; j < len(rf.peers); j++ {
								rf.nextindex[j] = lastlogindex + 1 // 所以假设没有新请求过来，第一次发空entry
								rf.matchindex[j] = 0
							}
							go rf.DoAppendEntries()
						}
					}
					rf.mu.Unlock()
				}(i, term, candidateindex, lastlogindex, lastlogterm)
			}
		}
		time.Sleep(rf.electiontimeout)
	}
}

type LockAppendEntriesArgs struct {
	entries []Log
	startindex int
	endindex int
	prevlogindex int
	prevlogterm int
}

func (rf *Raft) CalulateEntriesForPeers() []LockAppendEntriesArgs {
	var lockappendentriesargs []LockAppendEntriesArgs
	for nodeindex := 0; nodeindex < len(rf.peers); nodeindex++ {
		/*
			nextindex[nodeindex]被设置为len(rf.log)
			假设中间没有新日志提交，根据go的slice操作，rf.log[startindex:]第一次不发送任何日志。
			endindex代表假设返回true，follower的日志已经同步到了rf.log[endindex]
		*/
		startindex := rf.nextindex[nodeindex]
		entries := make([]Log, 0)
		// 以前在这个地方有时会index out of。我估计是因为作为老leader还在继续发appendEntries请求，但是同时作为follower又被
		// 新leader删除了部分日志。导致在访问rf.log[startindex:]越界。但是在这个lock的实现中，由于在发送给各个node之前就锁住
		// 了，entries在哪个时刻总是OK的，就不会出现这个问题了。
		entries = append(entries, rf.log[startindex:]...)
		endindex := startindex + len(entries) - 1
		prevlogindex := startindex - 1
		prevlogterm := rf.log[prevlogindex].Term

		lockappendentriesarg := LockAppendEntriesArgs{}
		lockappendentriesarg.startindex = startindex
		lockappendentriesarg.endindex = endindex
		lockappendentriesarg.entries = entries
		lockappendentriesarg.prevlogindex = prevlogindex
		lockappendentriesarg.prevlogterm = prevlogterm
		lockappendentriesargs = append(lockappendentriesargs, lockappendentriesarg)
	}
	return lockappendentriesargs
}

/*
	1.为什么在本任期内提交了至少一个entry，提交以前的就是安全的了？
	2.创建单独的goroutine()来提交log到状态机。这里的核心是：
		* 在lab3 kvraft中，rf.commitchan <- ApplyMsg可能会发生阻塞。在以前没有创建单独的gorountine()时，在阻塞期间，还会持有
		rf.mu的锁。即使fix了kv.server调用rf.Start()不加锁时，可能在某个地方也会发生四路死锁(我没有找到具体在哪)
		* 这个函数只有在获取要提交的日志时加锁，但是在rf.commitchan <- ApplyMsg不再使用rf.mu的锁，用了另外一个锁，另外一个锁是
		防止多次Signal的同时处理。
	3.可能依然有问题。假设Wait()以后处理了一个Signal，但是还没处理完又来了一个Signal，可能会丢失部分日志没有继续提交。
		* 可以设置一个定时任务，检查lastapplied和commitindex，发出Signal。
		* 设置一个Signal的个数，检查可能miss的Signal。
 */
func (rf *Raft) DoApplyLogs() {
	for {
		rf.commitmu.Lock()
		rf.commitcond.Wait()
		rf.mu.Lock()
		start := rf.lastapplied + 1
		end := rf.commitindex
		commitlogs := rf.log[start : end + 1]
		raftindex := rf.me
		currentterm := rf.currentterm
		role := RoleString(rf.role)
		DPrintf("%v: {term - %v role - %v} start - %v end - %v commitlogs - %+v",
			raftindex, currentterm, role, start, end, commitlogs)
		rf.mu.Unlock()
		for i := start; i <= end; i++ {
			DPrintf("%v: {term - %v role - %v} commit command %v at index %v\n",
				raftindex, currentterm, role, commitlogs[i - start].Command, i)
			rf.commitchan <- ApplyMsg{true, commitlogs[i - start].Command, i, commitlogs[i - start].Term}
		}
		rf.mu.Lock()
		rf.lastapplied = end
		rf.mu.Unlock()
		rf.commitmu.Unlock()
	}
}

func (rf *Raft) DoAppendEntries() {
	/*
		这个是DoHeartbeat的升级版。实际上，在raft中，要把心跳和replication log结合起来。
		考虑到这样的场景，leader一边在跟follower心跳维持地位，一边接受client的新请求。这样会出现一会发空日志，一会发append日志。
		1.没有要replicate的log的时候，发心跳包 。
		2.有要replicate的log的时候，发复制日志包。
		无限循环
		这个实现不好的地方在于，如果日志复制失败了需要等待较长的时间，一下复制一批。- heartbeatinterval
	*/
	for {
		rf.mu.Lock()
		if rf.role != LEADER {
			rf.mu.Unlock()
			break
		}
		term := rf.currentterm
		leaderid := rf.me
		leadercommitindex := rf.commitindex
		lockappendentriesargs := rf.CalulateEntriesForPeers()
		rf.mu.Unlock()
		for i := 0; i < len(rf.peers); i++ {
			// 向各个node发出RequestVote
			if i != rf.me {
				go func(nodeindex int, term int, leaderid int, leadercommitindex int, lockappendentriesargs []LockAppendEntriesArgs) {
					args := &AppendEntriesArgs{}
					reply := &AppendEntriesReply{}
					logid := rand.Int()
					args.LogId = logid
					args.Term = term
					args.LeaderId = leaderid
					args.LeaderCommitIndex = leadercommitindex
					args.PrevLogIndex = lockappendentriesargs[nodeindex].prevlogindex
					args.PrevLogTerm = lockappendentriesargs[nodeindex].prevlogterm
					args.Entries = lockappendentriesargs[nodeindex].entries
					reply.LogId = logid
					// 有日志发日志，没日志发心跳
					ok := rf.sendAppendEntries(nodeindex, args, reply)
					/*
						按照图2的说法，两种情况会返回reply.Success = false。
						1.follower的term比当前的还新。在sendAppendEntries会把当前这个leader变成follower，不会再转入下面的判断
						2.follower的日志没有完全，需要reply.Success = false以让leader发多余的日志给follower
					*/
					rf.mu.Lock()
					if ok && rf.role == LEADER && args.Term == rf.currentterm {
						if !reply.Success {
							/*
								在没有改变AppendEntriesReply的参数的情况下，需要一步步回退
								在下一次的循环中，多发一条日志给这个follower
							*/
							// rf.nextindex[nodeindex] = rf.nextindex[nodeindex] - 1
							if reply.ConflictIndex != -1 {
								rf.nextindex[nodeindex] = reply.ConflictIndex // fast back
							}
						} else {
							/*
								成功的复制了很多日志，更新nextindex/matchindex。
								这里要使用上面的endindex，因为len(rf.log)可能已经变了。
								nextindex更新为endindex+1，因为follower已经在rf.log[endindex]一样了，下次从endindex+1发同步日志
							*/
							rf.nextindex[nodeindex] = lockappendentriesargs[nodeindex].endindex + 1
							rf.matchindex[nodeindex] = lockappendentriesargs[nodeindex].endindex
							for iter := rf.commitindex + 1; iter < len(rf.log); iter++ {
								/*
									由于matchindex增加了，commitindex可能可以增加了。
									由于placeholder的存在，从位置1开始提交
								*/
								if rf.log[iter].Term != rf.currentterm {
									// 按照图2的说法，还有一个条件是log[iter].Term == rf.currentterm
									// 解决图8(c)问题，不会发生图8(c)的(term=2, index=2, cmd=2)被提交以后又被覆盖的问题。
									continue
								}
								// 本term内append的日志。
								count := 1 // 超过iter的个数，超过半数即可增加commitindex
								//DPrintf("[%v] %v : matchindex - %+v", logid, rf.me, rf.matchindex)
								for j := 0; j < len(rf.peers); j++ {
									if j != rf.me {
										if rf.matchindex[j] >= iter {
											count += 1
										}
									}
								}
								if count*2 > len(rf.peers) {
									rf.commitindex = iter
									DPrintf("[%v] %v: {term - %v role - %v} notify commit logs\n",
										logid, rf.me, rf.currentterm, RoleString(rf.role))
									rf.commitcond.Signal()
								} else {
									// 当前尝试的下标都不行，下面的更不行
									break
								}
							}
						}
					}
					rf.mu.Unlock()
				}(i,  term, leaderid, leadercommitindex, lockappendentriesargs)
			}
		}
		time.Sleep(rf.heartbeatinteval)
	}
}

//
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
//
/*
	Start()需要迅速返回，不要等待日志添加完成。也不保证此日志一定会提交。
	例如，leader接受了一个命令，append到本地日志，然后被网络隔离了。其他node会选举出新leader。
	等到此leader回到集群中，会发现本地不对，然后清除本地日志。
	每次commit了一个log，要发送ApplyMsg到applyCh里。
*/
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	/*
		用来debug死锁确实存在于raft node中，睡眠2s，如果还没有获取到锁，就在rf.logWithOutLock()停留。
	 */
	//ch := make(chan struct{})
	//go func() {
	//	select {
	//	case <-time.After(time.Second * 2):
	//		rf.logWithOutLock()
	//	case <-ch:
	//		return
	//	}
	//}()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//ch <- struct{}{}
	index := -1
	term := -1
	isLeader := true
	logid := rand.Int()
	// Your code here (2B).
	if rf.role != LEADER {
		// 不是leader，直接返回
		DPrintf("%v : {term - %v role - %v} : receive command %v, refuse\n",
			rf.me, rf.currentterm, RoleString(rf.role), command)
		isLeader = false
	} else {
		DPrintf("%v : {term - %v role - %v} : receive command %v, accept\n",
			rf.me, rf.currentterm, RoleString(rf.role), command)
		index = len(rf.log)
		term = rf.currentterm
		rf.log = append(rf.log, Log{rf.currentterm, command})
		//会被周期性的心跳自动解决。
		//go rf.DoAppendEntries()
		rf.persist()
		DPrintf("[%v] %v : accept command {index - %v term - %v command - %v}",
			logid, rf.me, index, rf.currentterm, command)
	}
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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
	rf.dead = 0
	rf.currentterm = 0
	rf.votedfor = -1
	rf.log = make([]Log, 0)
	/*
		这里有一个问题，就是空log怎么表示？
		1.现在的设计方法是认为空日志是 index=0,term=0，有一个日志的情况是index=0,term!=0
		2.设置placeholder，- 可以，但是
			2.1 对index的各种处理要特别足以。
			2.2 能简化上面的index=0的两种情况的判断问题。
			2.3 不需要提交index为0的entry，因为是placeholder。
	*/
	rf.log = append(rf.log, Log{0, -1})
	rf.commitindex = 0
	rf.lastapplied = 0
	rf.role = FOLLOWER
	rf.lastheartbeat = time.Now()
	// 随机化选举时间，心跳时间可以不用随机化
	rf.heartbeattimeout = time.Duration(150) * time.Millisecond
	// heartbeatinteval必须要小于rf.heartbeattimeout，否则某些candidate会重新开始选举
	rf.heartbeatinteval = time.Duration(50) * time.Millisecond
	randtime := rand.Intn(50)
	rf.electiontimeout = time.Duration(randtime + 200) * time.Millisecond
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.leaderid = -1
	rf.commitchan = applyCh
	/* 这里的核心是每个raft作为不同的角色，执行的操作不一样，导致这个goroutine很难统一的写，实际上在main loop里
	   只考虑需要考虑心跳的一种case。其他的放到另外的gorountine里去。
	   1.作为candidate，要持续的发起vote，如果出现了split vote或者long delay的情况。
	   2.作为follower，持续监听心跳，如果长时间没有接收到，转为candidate。
	   3.作为leader，持续定时发送心跳，维持自己的地位。
	*/
	logid := rand.Int()
	rf.commitcond = sync.NewCond(&rf.commitmu)
	go rf.DoApplyLogs()
	go func() {
		for {
			/*
				1.在集群的几个node一起起来的时候，sleep的时间初始基本接近rf.heartbeattimeout。
				2.当sleep途中收到一个心跳时，要调整睡眠时间。例如某个node重新加入的时候，可能在sleep中收到了心跳，则下次失眠要调整睡眠时间。
				3.当sleep途中没有收到心跳，这个设置会有问题，相当于没睡眠，所以要加入后面那个lastheartbeat重置条件。
				例如初始3个raft node一起启动，在睡眠了一段时间后没有收到心跳，触发选举。重新回到这个地方时，如果没有后面重置，由于lastheart还没修改过
				rf.heartbeattimeout - time.Since(rf.lastheartbeat)会是负值或者接近于0，导致此node马上又发起第二轮选举。
			*/
			//DPrintf("[%v] %v : sleep %v, at time %v, last heart beat %v\n",
			//	logid, rf.me, rf.heartbeattimeout-time.Since(rf.lastheartbeat), time.Since(programestarttime), time.Since(rf.lastheartbeat))
			rf.mu.Lock()
			sincelastheartbeat := time.Since(rf.lastheartbeat)
			rf.mu.Unlock()
			time.Sleep(rf.heartbeattimeout - sincelastheartbeat)
			rf.mu.Lock()
			DPrintf("[%v] %v : { term - %v role - %v entries - %+v commitindex - %v} wake up at time %v\n",
				logid, rf.me, rf.currentterm, RoleString(rf.role), rf.log, rf.commitindex, time.Now())
			// 这里要考虑sleep以后的rf.lastheartbeat被更新了。如果在整个heartbeattimeout期间发现新的心跳，说明要转为
			// CANDIDATE，并发起投票。如果不是FOLLOWER，就不管，继续睡眠
			if rf.role == FOLLOWER && time.Since(rf.lastheartbeat) > rf.heartbeattimeout {
				//rf.mu.Lock()
				rf.role = CANDIDATE
				//rf.mu.Unlock()
				go rf.DoVote()
			}
			if time.Since(rf.lastheartbeat) >= rf.heartbeattimeout {
				rf.lastheartbeat = time.Now()
			}
			rf.mu.Unlock()
		}
	}()

	return rf
}

/*
	怎么样通过go test -race
	1.对于RPC，应该使用defer rf.mu.Unlock()
	2.对于Sleep, sendRequestVote()之类的，由于会阻塞，不应该让lock跨越整个函数体。
	3.leader发送vote/appendEntries，要注意不能在goroutine通过rf来获取，应该在外部加锁获取，并且作为参数传入。否则会导致问题。
	因为发送给不同的node的参数已经不是一样了，例如term已经变了。详情可见rule5:https://pdos.csail.mit.edu/6.824/labs/raft-locking.txt
	reply也要考虑这种问题，这就是为什么check回复时，要args.Term == rf.currentterm。
	这也是为什么要设计lockAppendEntriesArg的struct
*/

/*
	结构问题 - 本实验采用的sleep的方式。实际上通过channel的方式应该也可以组织，可能更好。
	周期性激发heartbeattimeout，如果收到表示要发起reqeustVote()
	接收端
	for {
		select {
			case <- heartbeatchan:
				{
					fmt.Println("receive heartbeat from leader")
				}
			case <- time.After(rf.heartbeattimeout):
				{
					fmt.Println("receive heartbeat time out event, start to vote")
					...
				}
			case <- time.After(rf.electiontimeout):
				{
					fmt.Println("election time out event, start to a new vote")
					...
				}
		}
	}
}
 */


func (rf *Raft) logWithOutLock() {
	DPrintf("rf[%v] deadlock", rf.me)
}

