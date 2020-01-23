package kvraft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"sync/atomic"
	"time"
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

	// lab3B
	persister *raft.Persister
}

/*
	Get()请求做了一个假的Op来提交，如果可以提交成功，则代表它的database数据是OK的。
*/
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	/*
		kv.mu.Lock()
		四路死锁，以下是死锁的场景：
		1.Get请求发起rf.Start() - 此时拿着kv的mu。
		2.rf.start()等待rf的mu。
		3.rf正在调用DoApplyLogs()
			rf.commitchan <- ApplyMsg{true, rf.log[i].Command, i, rf.log[i].Term}
		4.上述的commit会导致只有commitchan读取，多条才能继续插入，然而kv的ListenToRaftCommit()函数在取完一条commit信息以后
			case msg := <- kv.applyCh:
			{
				kv.mu.Lock()
				...
			}
		由于往下处理需要获取kv的mu，所以锁住了。
		所以这就是个死锁的情况。在TestBasic3A()有时就会发生。
		这个四路死锁的问题在于：如果要在raft之上做运用，就要特别仔细。
	*/
	//
	op := Op{"Get", args.Key, "", args.ClientId, args.SeqId, 0}
	index, term, isleader := kv.rf.Start(op)
	if isleader {
		DPrintf("KVServer[%v] Get : op - %+v index - %v term - %v isleader - %v",
			kv.me, op, index, term, isleader)
	}
	if !isleader {
		reply.Err = ErrWrongLeader
		return
	}
	op.CommitTerm = term
	ch := kv.GetIndexChan(index)
	var commitop Op
	select {
	case commitop = <-ch:
		//close(ch)
		// 当前的leadre可以提交日志，说明可以联系半数以上的
	case <-time.After(kv.timeout):
		// 超时了，相当于失败
		DPrintf("KVServer[%v] Get : timeout, op - %+v",
			kv.me, op)
		reply.Err = ErrWrongLeader
		return
	}
	if !sameOp(op, commitop) {
		DPrintf("KVServer[%v] WaitForCommit : different op, commitop - %+v expectedop - %+v",
			kv.me, commitop, op)
		reply.Err = ErrWrongLeader
		return
	}
	// if key not exist, just return "" or return ErrNoKey
	kv.mu.Lock()
	value, ok := kv.database[args.Key]
	if ok {
		reply.Err = OK
		reply.Value = value
	} else {
		reply.Err = ErrNoKey
		reply.Value = ""
	}
	kv.mu.Unlock()
}

/*
	判断最终提交和调用Start()时的参数。如果leader没变，实际上index, term和clientId/SeqId应该是一样的。
	Key和Value也得判断，因为对于Get()请求时，mock的假log的ClientId/SeqId是0。
	如果Get()也带这两个字段，应该可以不用比较Key/Value。
*/
func sameOp(expectedop Op, actualop Op) bool {
	return expectedop.CommitTerm == actualop.CommitTerm &&
		expectedop.ClientId == actualop.ClientId &&
		expectedop.SeqId == actualop.SeqId &&
		expectedop.Key == actualop.Key &&
		expectedop.Value == actualop.Value &&
		expectedop.Op == actualop.Op
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

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{args.Op, args.Key, args.Value, args.ClientId, args.SeqId, 0}
	//kv.mu.Lock()
	//maxseqid, ok := kv.clientid2maxseqid[op.ClientId]
	//if ok && op.SeqId <= maxseqid {
	//	reply.Err = OK
	//}
	//kv.mu.Unlock()
	index, term, isleader := kv.rf.Start(op)
	if isleader {
		if op.Op == "Put" {
			DPrintf("KVServer[%v] Put : op - %+v index - %v term - %v isleader - %v",
				kv.me, op, index, term, isleader)
		} else {
			DPrintf("KVServer[%v] Append : op - %+v index - %v term - %v isleader - %v",
				kv.me, op, index, term, isleader)
		}
	}
	if !isleader {
		reply.Err = ErrWrongLeader
		return
	}
	op.CommitTerm = term
	ch := kv.GetIndexChan(index)
	var commitop Op
	select {
	case <-time.After(kv.timeout):
		{
			DPrintf("KVServer[%v] WaitForCommit : op %+v timeout",
				kv.me, op)
			reply.Err = ErrWrongLeader
			return
		}
	case commitop = <-ch:
		{
			// ListenToRaftCommit 会将此index提交的op传过来
			DPrintf("KVServer[%v] WaitForCommit : index %v commit op %+v expected op - %+v",
				kv.me, index, commitop, op)
			//close(ch)
		}
	}
	if !sameOp(op, commitop) {
		DPrintf("KVServer[%v] WaitForCommit : different op, commitop - %+v expectedop - %+v",
			kv.me, commitop, op)
		reply.Err = ErrWrongLeader
		return
	}
	reply.Err = OK
	return
}

/*
	不断监听Raft的提交请求，然后根据commitindex分发到不同的chan
*/
func (kv *KVServer) ListenToRaftCommit() {
	for {
		select {
		case msg := <-kv.applyCh:
			{
				if !msg.CommandValid {
					/*
						关于snapshot这里有个问题，就是假设由于网络原因，假设生成了一个snapshot，然后有append了一个log。
						正确的做法是先apply snapshot, 再apply log，但是乱序发生了，log先发过来，写db。然后快照来了，
						db被完全覆盖了，这样前面那条log就被丢失了。
						这种情况怎么解决呢？
					 */
					buf := msg.Command.([]byte)
					kv.mu.Lock()
					// restore kvDB and cid2seq
					kv.clientid2maxseqid, kv.database = kv.DeserializeKVState(buf)
					kv.mu.Unlock()
				} else {
					// 日志请求
					kv.mu.Lock()
					op := msg.Command.(Op)
					op.CommitTerm = msg.CommitTerm
					/*
						初始的时候，我打算把这个检查放在PutAppend()里，但是这是错误的。因为:
						1.在commit的时候才能设置clientid2maxseqid。
						2.在raft实验中，我发现重启的Raft会重复提交log entry，这个时候应该在这里处理，在应用到状态机(本例中
						就是这个kv存储时，要丢掉这个重复提交的记录)
						3.PutAppend可以只检查不更新不？
					*/
					maxseqid, ok := kv.clientid2maxseqid[op.ClientId]
					if !ok || op.SeqId > maxseqid {
						DPrintf("KVServer[%v] update clientid - %v seqid from %v to %v",
							kv.me, op.ClientId, maxseqid, op.SeqId)
						kv.clientid2maxseqid[op.ClientId] = op.SeqId
						op.CommitTerm = msg.CommitTerm
						if op.Op == "Put" {
							DPrintf("KVServer[%v] Put overwrite database key - %v from [%v] to [%v]",
								kv.me, op.Key, kv.database[op.Key], op.Value)
							kv.database[op.Key] = op.Value
						} else {
							DPrintf("KVServer[%v] Append update database key - %v from [%v] to [%v]",
								kv.me, op.Key, kv.database[op.Key], kv.database[op.Key]+op.Value)
							kv.database[op.Key] += op.Value
						}
					}
					kv.CheckAndGenerateSnapshot(msg.CommandIndex)
					kv.mu.Unlock()
					kv.GetIndexChan(msg.CommandIndex) <- op
				}
			}
		}
	}
}

func (kv *KVServer) CheckAndGenerateSnapshot(commitindex int) {
	raftstatesize := kv.persister.RaftStateSize()
	limitsize :=  int(0.8 * float32(kv.maxraftstate))
	DPrintf("KVServer[%v] maxraftstatesize - %v, currentraftstate size - %v limitsize - %v",
		kv.me, kv.maxraftstate, raftstatesize, limitsize)
	if kv.maxraftstate == -1 || raftstatesize < limitsize {
		// 90% 阈值时就要发起GenerateSnapshot()
		return
	}
	DPrintf("KVServer[%v] start to generate snapshot ",
		kv.me)
	/*
		最开始我打算把kvstate也持久化放在另外一个goroutine()，但是实际上不行。因为假设ListenToRaftCommit()连续监听到
		两个commit，第一个commit在goroutine()里传入commitindex和kv的snapshot可能对不上了。因为可能ListenToRaftCommit()
		又加入了一个新的commit的数据。
		上面的情况，从结果看好像没问题，就是那个commit在恢复时，又重新写了一次。
	*/
	kvstate := kv.SerializeKVState()
	go kv.rf.GenerateSnapshot(kvstate, commitindex)
}

func (kv *KVServer) DeserializeKVState(kvstate []byte) (map[int64]int64, map[string]string) {
	r := bytes.NewBuffer(kvstate)
	d := labgob.NewDecoder(r)
	var clientid2maxseqid map[int64]int64
	var database map[string]string
	if d.Decode(&clientid2maxseqid) != nil || d.Decode(&database) != nil {
		DPrintf("decode snapshot error !")
	}
	return clientid2maxseqid, database
}

func (kv *KVServer) SerializeKVState() []byte {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	if err := e.Encode(kv.clientid2maxseqid); err != nil {
		panic(fmt.Errorf("KVServer[%v] encode cid2sclientid2maxseqideq fail: %v", kv.me, err))
	}
	if err := e.Encode(kv.database); err != nil {
		panic(fmt.Errorf("KVServer[%v] encode database fail: %v", kv.me, err))
	}
	return w.Bytes()
}

func (kv *KVServer) LoadSnapshot(kvstate []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kvstate != nil || len(kvstate) != 0 {
		DPrintf("KVServer[%v] find snapshot, recover from snapshot", kv.me)
		kv.clientid2maxseqid, kv.database = kv.DeserializeKVState(kvstate) // restore kvDB and cid2seq
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
	kv.persister = persister
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.clientid2maxseqid = make(map[int64]int64)
	kv.database = make(map[string]string)
	kv.index2chanop = make(map[int]chan Op)
	kv.timeout = time.Duration(200) * time.Millisecond
	kv.LoadSnapshot(kv.persister.ReadSnapshot())
	go kv.ListenToRaftCommit()
	return kv
}
