package kvraft

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"sync/atomic"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op    string
	Key   string
	Value string
	// 下面这几个主要用来看是否leader变了的
	ClientId   int64
	SeqId      int64
	CommitTerm int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	// 记录要不要直接回复client的请求，避免duplicate处理。实际上中，要考虑KVServer的这个字段的持久化。否则会导致有些op
	// 重复执行，导致某个时间段数据不一致。
	clientid2maxseqid map[int64]int64
	// 使用map模拟一个kv数据库
	database map[string]string
	// 记录了index->chan op的映射
	index2chanop map[int]chan Op
	// 操作超时时间
	timeout time.Duration
}

/*
	Get()请求做了一个假的Op来提交，如果可以提交成功，则代表它的database数据是OK的。
 */
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{"Get", args.Key, "", args.ClientId, args.SeqId}
	idx, term, isleader := kv.rf.Start(op)
	if !isleader {
		reply.Err = ErrWrongLeader
		return
	}
	op.CommitTerm = term
	ch := kv.GetIndexChan(idx)
	select {
	case commitop = <-ch:
		// 当前的leadre可以提交日志，说明可以联系半数以上的
		if !sameOp(op, commitop) {
			reply.Err = ErrWrongLeader
			return
		}
	case <-time.After(kv.timeout):
		// 超时了，相当于失败
		reply.Err = ErrWrongLeader
		return
	}

	// if key not exist, just return "" or return ErrNoKey
	value, ok := kv.database[args.Key]
	if ok {
		reply.Value = kv.database[args.Key]
	} else {
		reply.Err = ErrNoKey
		reply.Value = ""
	}
}

/*
	判断最终提交和调用Start()时的参数。如果leader没变，实际上index, term和clientId/SeqId应该是一样的。
 */
func sameOp(expectedop Op, actualop Op) bool {
	return expectedop.CommitTerm == actualop.CommitTerm &&
		expectedop.ClientId == actualop.ClientId &&
		expectedop.SeqId == actualop.SeqId
}

/*
	本来我是想在WaitForCommit()里创建index提交的chan的，但是实际上不行。因为：
	在极端情况下，Start()执行完以后，Raft迅速达成一致并提交了entry。如果ListenToRaftCommit()先运行，会导致chan还没创建。
	所以WaitForCommint()和ListenToRaftCommit()都调用此函数创建chan。
*/
func (kv *KVServer) GetIndexChan(idx int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, ok := kv.index2chanop[idx]
	if !ok {
		ch = make(chan Op, 1) // never block this
		kv.index2chanop[idx] = ch
	}
	return ch
}

func (kv *KVServer) WaitForCommit(index int, op Op) bool {
	select {
	case <-time.After(kv.timeout):
		{
			// 超时，怎么处理？
			return false
		}
	case commitop <- kv.GetIndexChan(index):
		{
			// ListenToRaftCommit 会将此index提交的op传过来
			if sameOp(op, commitop) {
				return true
			} else {
				return false
			}
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{args.Op, args.Key, args.Value, args.ClientId, args.SeqId}
	index, term, isleader := kv.rf.Start(op)
	if !isleader {
		reply.Err = ErrWrongLeader
		return
	}
	op.CommitTerm = term
	commitok := kv.WaitForCommit(index, op)
	if !commintok {
		/*
			超时
			在期望的indx提交的是另外一个请求
		*/
		reply.Err = ErrWrongLeader
	} else {
		reply.Err = OK
	}
}

/*
	不断监听Raft的提交请求，然后根据commitindex分发到不同的chan
*/
func (kv *KVServer) ListenToRaftCommit() {
	for {
		select {
		case msg <- kv.applyCh:
			{
				// 监听raft的消息，并且分发请求给index2chanop
				op := msg.Command.(Op)
				/*
					初始的时候，我打算把这个检查放在PutAppend()里，但是这是错误的。因为:
					1.在commit的时候才能设置clientid2maxseqid。
					2.在raft实验中，我发现重启的Raft会重复提交log entry，这个时候应该在这里处理，在应用到状态机(本例中
					就是这个kv存储时，要丢掉这个重复提交的记录)
				*/
				maxseqid, ok := kv.clientid2maxseqid[op.ClientId]
				if !ok || op.SeqId > maxseqid {
					kv.clientid2maxseqid[op.ClientId] = maxseqid
					op.CommitTerm = msg.CommitTerm
					if op.Op == "Put" {
						kv.database[op.Key] = op.Value
					} else {
						kv.database[op.Key] += op.Value
					}
				}
				kv.GetIndexChan(msg.CommandIndex) <- op
			}
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.timeout = time.Duration(200) * time.Millisecond
	go kv.ListenToRaftCommit()
	return kv
}
