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

var programestarttime time.Time

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
	commitindex int // 将被提交的日志记录的索引(初值为0且单调递增)
	lastapplied int // 已经被提交到状态机的最后一个日志的索引(初值为0且单调递增)

	// leader可变状态
	// 对于某个server来说，leader需要发送给那个server的index，初始化为leader的last log index + 1。数组长度跟peers长度一样。
	// 不断的试探回退。
	nextindex []int
	// 每个server log里最高的匹配leader log的index，初始都为0。
	matchindex []int // 跟peers长度一样

	// 其他未在图二中有的，但是我觉得需要
	role             int
	lastheartbeat    time.Time     // 记录上次的心跳时间
	heartbeattimeout time.Duration // 心跳检测的时间
	electiontimeout  time.Duration // 选举超时时间
	heartbeatinteval time.Duration // 用于leader周期性发心跳，在论文中，并没有指明需不需要这个值。只是说在空闲时间发心跳，如果设置为hearttimeout，会有问题。
	//heartbeatticker  *time.Ticker  //用于leader周期性激活
	//electionticker   *time.Ticker  // 用于选举的周期性激活
	leaderid         int           // 当前的leader
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
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
	LogId     int // debug
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 这里应该是其他peer当前的term，用于返回给发起投票的candidate更新。
	VoteGranted bool // 是否收到了同意的投票
	LogId     int // debug
}

// ugly，怎么使用golang写的更好
func LogNewer(candidatelogindex int, candidatelogterm int, mylogindex int, mylogterm int) bool {
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
		DPrintf("[%v]- %v %v {term - %v role - %v}： refuse vote {index - %v term - %v }\n",
			logid, rf.me, rf.currentterm, rf.role, args.CandidateIndex, args.Term)
		reply.VoteGranted = false
	} else {
		if args.Term > rf.currentterm {
			DPrintf("[%v]%v : {term - %v role - %v}: - change term because recevie vote {index - %v term - %v }\n",
				logid, rf.me, rf.currentterm, rf.role, args.CandidateIndex, args.Term)
			rf.role = FOLLOWER
			rf.currentterm = args.Term
			rf.votedfor = -1
		}
		//candidatelogindex := args.LastLogIndex
		//candidatelogterm := args.LastLogItemTerm
		//mylogindex := len(rf.log) - 1
		//mylogterm := rf.log[mylogindex].Term
		//// 计算谁的日志更新
		//candidatenewer := LogNewer(candidatelogindex, candidatelogterm, mylogindex, mylogterm)
		voteconditon := false
		candidatenewer := true
		if rf.votedfor == -1 || rf.votedfor == args.CandidateIndex {
			// 没有为最新的term投过票或者投过相同的票了？第二个条件是因为可能出现发送两次请求吗？- 包重复
			//DPrintf("%v - %v votecondition is true because votefor is %v\n", args.CandidateIndex, rf.me, rf.votedfor)
			voteconditon = true
		}
		if voteconditon && candidatenewer {
			//DPrintf("accept %v - %v\n", args.CandidateIndex, rf.me)
			reply.VoteGranted = true
			rf.votedfor = args.CandidateIndex
			rf.role = FOLLOWER // 从图表中好像没有这个设置?需要这个地方吗？
		} else {
			//DPrintf("refuse %v - %v\n", args.CandidateIndex, rf.me)
			reply.VoteGranted = false
		}
	}
	if reply.VoteGranted {
		DPrintf("[%v] %v {term - %v role - %v}: accept vote from {index - %v term - %v }",
			logid, rf.me, rf.currentterm, rf.role, args.CandidateIndex, args.Term)
	} else {
		DPrintf("[%v] %v {term - %v role - %v}: refuse vote from {index - %v term - %v }",
			logid, rf.me, rf.currentterm, rf.role, args.CandidateIndex, args.Term)
	}
	reply.Term = rf.currentterm
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
1.例如sendRequestVote()发送出去以后，收到的回复可能是long delay，也可能是丢失或者乱序等，但是不会requestVote()出去,
appendEntries()的回应回来
2.不会是sendRequestVote1()请求出去，sendRequestVote2()的reply回来。
3.这里的乱序指的是，sendRequestVote1先发，sendRequestVote2后发，但是可能sendRequestVote2的回复先回来。
4.考虑到通信都是使用一个端口，是不是上面的理解错误了？testing的代码也需要看，确认这些错误的行为包括哪些。
*/
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	logid := args.LogId
	DPrintf("[%v] %v: sendRequestVote to %v request, time - %v, args - %+v\n",
		logid, args.CandidateIndex, server, time.Since(programestarttime), args)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	DPrintf("[%v] %v : sendRequestVote receive %v reply, time - %v, args - %+v reply - %+v\n",
		logid, rf.me, server, time.Since(programestarttime), args, reply)
	if reply.Term > rf.currentterm {
		rf.mu.Lock()
		rf.currentterm = reply.Term
		rf.role = FOLLOWER
		rf.votedfor = -1
		rf.mu.Unlock()
	}
	return ok
}

type AppendEntriesArgs struct {
	Term         int // leader的term
	LeaderId     int // leader的index, follower可以设置，设置在哪个字段？
	PrevLogIndex int //
	PrevLogTerm  int // 上面对应的term

	Entries           []Log // 待复制的日志
	LeaderCommitIndex int   // leader的commit index
	LogId     int // debug
}

type AppendEntriesReply struct {
	Term    int  // 假设follower的term比leader还高，要回复这个term。leader会根据这个值重置自己的term?
	Success bool // 如果follower包含索引为prevLogIndex，且任期为prevLogTerm。
	LogId     int // debug
}

// 处理心跳和复制日志
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// 仅仅是lab2A
	rf.mu.Lock()
	defer rf.mu.Unlock()
	logid := args.LogId
	DPrintf("[%v] %v: {term - %v role - %v} receive TBD heart beat from %v, args - %+v\n",
		logid, rf.me, rf.currentterm, rf.role, args.LeaderId, args)
	if args.Term < rf.currentterm {
		DPrintf("[%v] %v: {term - %v role - %v} : receive invalid heart beat from %v, args - %+v\n",
			logid, rf.me, rf.currentterm, rf.role, args.LeaderId, args)
		reply.Success = false
	} else if args.Term > rf.currentterm {
		// node从网络隔离中新上线。
		DPrintf("[%v] %v: {term - %v role - %v} : receive valid heart beat from %v, args - %+v\n",
			logid, rf.me, rf.currentterm,rf.role, args.LeaderId, args)
		reply.Success = true
		rf.currentterm = args.Term
		rf.role = FOLLOWER
		rf.votedfor = args.LeaderId
		rf.leaderid = args.LeaderId
	} else {
		/*
			场景
			1.node1和node2同时发起RequestVote，node1变为leader，node2还在等待vote的回复，接着node1开始发心跳，node2接到
			此心跳，需要更新node2的leaderid。
			2.node1当选为leader后的常规心跳，不用更新node2的leaderid。
		 */
		//如果本身是candidate，是不是应该也要转换role?
		DPrintf("[%v] %v: {term - %v role - %v} : receive valid heart beat from %v, args - %+v\n",
			logid, rf.me, rf.currentterm, rf.role, args.LeaderId, args)
		rf.role = FOLLOWER
		reply.Success = true
		// 下面这行漏掉了，如果
		rf.leaderid = args.LeaderId
	}
	if reply.Success {
		// 收到心跳包，更新lastHeart
		DPrintf("[%v] %v: {term - %v role - %v} update last heart beat because receive from %v, " +
			"last heart beat - %v, %v from last heart beat\n", logid, rf.me, rf.currentterm, rf.role, args.LeaderId, rf.lastheartbeat, time.Since(rf.lastheartbeat))
		rf.lastheartbeat = time.Now()
	}
	reply.Term = rf.currentterm
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	logid := args.LogId
	DPrintf("[%v] %v: sendAppendEntries to %v request, time - %v, args - %+v\n",
		logid, rf.me, server, time.Since(programestarttime), args)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	DPrintf("[%v] %v: sendAppendEntries receive %v reply, time - %v, args - %+v, reply - %+v\n",
		logid, rf.me, server, time.Since(programestarttime), args, reply)
	if reply.Term > rf.currentterm {
		rf.mu.Lock()
		rf.currentterm = reply.Term
		rf.role = FOLLOWER
		rf.votedfor = -1
		rf.mu.Unlock()
	}
	return ok
}

func (rf *Raft) DoHeartBeat() {
	// 在参考资料中，加入了一个进入的线程数检测，但是我觉得不需要，因为应该只会有一个线程能成功调用DoHeartBeat()
	for {
		if rf.role != LEADER {
			// 不再发起心跳
			break
		}
		for i := 0; i < len(rf.peers); i++ {
			/*
				向各个node发出AppendEntries
				prevlogindex - 这个参数为什么不传当前的logindex，我认为是为了统一heart beat和replciate log都用一个API。
				在replicate log的流程中，leader先应用到一条日志到本地，这样curlog index肯定是其他follower没有的。这样按照
				协议，follower直接返回false。
				preflogterm - 与上条配套使用。
				entries
				leadercommit
			*/
			if i != rf.me {
				go func(nodeindex int) {
					rf.mu.Lock()
					args := &AppendEntriesArgs{}
					// 其实在论文中，并没有交代心跳包的请求怎么构建。仅仅说明了entries里为空。
					args.Term = rf.currentterm
					args.LeaderId = rf.me
					args.PrevLogIndex = len(rf.log) - 1
					args.PrevLogTerm = 0          // 在lab2中应该只是占位符而已
					args.Entries = make([]Log, 0) // 空log
					args.LeaderCommitIndex = rf.commitindex
					rf.mu.Unlock()
					reply := &AppendEntriesReply{}
					logid := rand.Int()
					args.LogId = logid
					reply.LogId = logid
					// 发起心跳
					rf.sendAppendEntries(nodeindex, args, reply)
					// 应该不用取处理失败的情况，某一个失败，会继续向其他的node发送。
					// 例如一个long delay, 应该会导致一个日志足够新的node发起选举，有可能会产生新的leader
				}(i)
			}
		}
		/*
			time.Sleep(rf.heartbeattimeout)
			感觉这个发心跳还不能这么写，由于在main loop里面的sleep时间调整，会导致leader发心跳的时间总是接近于follower检查心跳的时间
			这样很容易leader发心跳的时间晚于其他follower检查心跳的时间一点点导致其他follower重新发起选举，导致很容易触发多次选举。
			感觉原则上这个sleep应该比检查心跳时间的时间短一些。
			所以我觉得应该还有一个heartbeatinteval的变量控制才对。
		*/
		time.Sleep(rf.heartbeatinteval)
	}
}

// 发起投票
func (rf *Raft) DoVote() {
	for {
		/* 每过electontimeout的时间检测一下自己是否变为了leader，或者其他人变成了leader。
		自己变为了leader -> rf.role = LEADER
		其他人变成了leader -> rf.role = FOLLOWER
		*/
		if rf.role != CANDIDATE {
			// 不再发起投票
			break
		}
		// 再发起一轮投票，前一轮如果是long delay也不作数了。
		rf.mu.Lock()
		var voteagree int64 = 1 // 自己先投自己一票
		rf.votedfor = rf.me
		rf.currentterm += 1
		DPrintf("%v : change term from %v to %v, start to vote\n", rf.me, rf.currentterm - 1, rf.currentterm)
		//term := rf.currentterm + 1 这个写法是错误的，自己的currentterm要+1
		term := rf.currentterm
		candidateindex := rf.me
		lastlogindex := len(rf.log) - 1
		lastlogterm := rf.log[lastlogindex].Term
		rf.mu.Unlock()
		for i := 0; i < len(rf.peers); i++ {
			// 向各个node发出RequestVote
			if i != rf.me {
				go func(nodeindex int, term int, candidateindex int, lastlogindex int, lastlogterm int) {
					args := &RequestVoteArgs{}
					args.Term = term
					args.CandidateIndex = candidateindex
					args.LastLogIndex = lastlogindex
					args.LastLogItemTerm = lastlogterm
					logid := rand.Int()
					args.LogId = logid
					reply := &RequestVoteReply{}
					reply.LogId = logid
					ok := rf.sendRequestVote(nodeindex, args, reply)
					rf.mu.Lock()
					/*
					 1.args.Term == rf.currentterm 应该是代表在candidate在投票期间，接收到了其他leader的appendEntries()
					 发现自己term比较小，更新了自己的currentterm。
					 2.rf.role == CANDIDATE 的检查在于，如果已经收到了足够的选票，在语句中修改了rf.role = LEADER，后续收到
					 yes回复的不再执行变为leader后做的事情。
					 3.如果reply.VoteGranted为false，此raft是否要更新自己的currentterm到reply.term ?
					*/
					//DPrintf("{ok - %v votegranted - %v role - %v arg.term - %v rf.term - %v }\n", ok, reply.VoteGranted, rf.role, args.Term, rf.currentterm)
					if ok && reply.VoteGranted && rf.role == CANDIDATE && args.Term == rf.currentterm {
						// 如果获取了一个选票
						DPrintf("[%v] %v : get one vote from %v", logid, rf.me, nodeindex)
						atomic.AddInt64(&voteagree, 1)
						// 如果获取了足够的投票转为leader，不足够仅仅voteagree++
						if int(voteagree)*2 > len(rf.peers) {
							DPrintf("[%v] %v : become leader", logid, rf.me)
							rf.leaderid = rf.me
							rf.role = LEADER
							// 重置leader的nextindex和matchindex
							// 按照规则，重置为当前candidate的最大logindex + 1
							rf.nextindex = make([]int, len(rf.peers))
							lastlogindex := len(rf.log) - 1
							for j := 0; j < len(rf.peers); j++ {
								rf.nextindex[j] = lastlogindex + 1
							}
							rf.matchindex = make([]int, len(rf.peers))
							for k := 0; k < len(rf.peers); k++ {
								rf.matchindex[k] = 0
							}
							// 发送心跳，维持leader地位。怎么实现？
							go rf.DoHeartBeat()
						}
					}
					rf.mu.Unlock()
				}(i, term, candidateindex, lastlogindex, lastlogterm)
			}
		}
		time.Sleep(rf.electiontimeout)
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
// Start()需要迅速返回，不要等待日志添加完成。
// 要求每次commit了一个log，要发送ApplyMsg到applyCh里。
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

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
// 产生各种goroutines来完成长时间任务，例如heart beat check等。
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	programestarttime = time.Now()

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.dead = 0
	// Your initialization code here (2A, 2B, 2C).
	// currentterm, votedfor, log都应该从磁盘上读取。
	// 但是为了先运行lab2a,需要先手动的处理。
	rf.currentterm = 0
	rf.votedfor = -1
	rf.log = make([]Log, 0)
	rf.log = append(rf.log, Log{1, "x->3"})

	rf.commitindex = 0
	rf.lastapplied = 0
	// nextindex, matchindex变为leader再使用
	rf.role = FOLLOWER
	rf.lastheartbeat = time.Now()
	// 随机化选举时间，心跳时间我觉得不需要随机化吧？这两个时间之间的关系有什么要求?
	rf.heartbeattimeout = time.Duration(120) * time.Millisecond
	// heartbeatinteval必须要小于rf.heartbeattimeout
	rf.heartbeatinteval = time.Duration(100) * time.Millisecond
	randtime := rand.Intn(50)
	rf.electiontimeout = time.Duration(randtime+150) * time.Millisecond
	//rf.heartbeatticker = time.NewTicker(rf.heartbeattimeout)
	//rf.electionticker = time.NewTicker(rf.electiontimeout)

	rf.leaderid = -1
	/* 这里的核心是每个raft作为不同的角色，执行的操作不一样，导致这个goroutine很难统一的写，实际上在main loop里
	   只考虑需要考虑心跳的一种case。其他的放到另外的gorountine里去。
	   1.作为candidate，要持续的发起vote，如果出现了split vote或者long delay的情况。
	   2.作为follower，持续监听心跳，如果长时间没有接收到，转为candidate。
	   3.作为leader，持续定时发送心跳，维持自己的地位。
	*/
	logid := rand.Int()
	DPrintf("[%v] %v: %+v }\n", logid, rf.me, rf)
	go func() {
		for {
			/*
				1.在集群的几个node一起起来的时候，sleep的时间初始基本接近rf.heartbeattimeout。
				2.当sleep途中收到一个心跳时，要调整睡眠时间。例如某个node重新加入的时候，可能在sleep中收到了心跳，则下次失眠要调整睡眠时间。
				3.当sleep途中没有收到心跳，这个设置会有问题，相当于没睡眠，所以要加入后面那个lastheartbeat重置条件。
				例如初始3个raft node一起启动，在睡眠了一段时间后没有收到心跳，触发选举。重新回到这个地方时，如果没有后面重置，由于lastheart还没修改过
				rf.heartbeattimeout - time.Since(rf.lastheartbeat)会是负值或者接近于0，导致此node马上又发起第二轮选举。
			*/
			DPrintf("[%v] %v : sleep %v, at time %v, last heart beat %v\n",
				logid, rf.me, rf.heartbeattimeout-time.Since(rf.lastheartbeat), time.Since(programestarttime), time.Since(rf.lastheartbeat))
			time.Sleep(rf.heartbeattimeout - time.Since(rf.lastheartbeat))
			DPrintf("[%v] %v : wake up at time %v\n", logid, rf.me, time.Since(programestarttime))
			// 这里要考虑sleep以后的rf.lastheartbeat被更新了。如果在整个heartbeattimeout期间发现新的心跳，说明要转为
			// candiate，并发起投票。
			// 如果不是FOLLOWER，就不管，继续睡眠
			if rf.role == FOLLOWER && time.Since(rf.lastheartbeat) > rf.heartbeattimeout {
				rf.mu.Lock()
				//DPrintf("raft[%v][term:%v] start to vote, at time %v\n", rf.me, rf.currentterm, time.Since(programestarttime))
				rf.role = CANDIDATE
				rf.mu.Unlock()
				go rf.DoVote()
			}
			if time.Since(rf.lastheartbeat) >= rf.heartbeattimeout {
				rf.lastheartbeat = time.Now()
			}
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}