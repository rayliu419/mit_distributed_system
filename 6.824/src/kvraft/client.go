package kvraft

import (
	"labrpc"
)
import "crypto/rand"
import "math/big"
import "sync/atomic"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader   int64
	clientid int64
	seqid    int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leader = -1
	ck.clientid = nrand()
	ck.seqid = nrand()
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//

func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	getargs := &GetArgs{}
	getreply := &GetReply{}
	getargs.Key = key
	leaderindex := atomic.LoadInt64(&ck.leader)
	for {
		if leaderindex == -1 {
			// 如果没有记录leader
			leaderindex = nrand() % int64(len(ck.servers))
		}
		ok := ck.servers[leaderindex].Call("KVServer.Get", getargs, getreply)
		if !ok || getreply.Err == ErrWrongLeader {
			// 请求失败，需要请求其他的server
			DPrintf("Clerk[%v] Get Fail, change leader : args - %+v reply - %+v leaderindex - %v ok - %v",
				key, getargs, getreply, leaderindex, ok)
			leaderindex = (leaderindex + 1) % int64(len(ck.servers))
			continue
		}
		// 可能还没有设定clerk的leader，设定它
		DPrintf("Clerk[%v] Get OK : args - %+v reply - %+v leaderindex - %v ok - %v",
			key, getargs, getreply, leaderindex, ok)
		atomic.StoreInt64(&ck.leader, leaderindex)
		return getreply.Value
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	putappendargs := &PutAppendArgs{}
	putappendreply := &PutAppendReply{}
	putappendargs.Op = op
	putappendargs.Key = key
	putappendargs.Value = value
	putappendargs.ClientId = ck.clientid
	putappendargs.SeqId = ck.seqid
	// 下一个次的PutAppend用新的SeqId
	atomic.AddInt64(&ck.seqid, 1)
	leaderindex := atomic.LoadInt64(&ck.leader)
	for {
		if leaderindex == -1 {
			// 如果没有记录leader
			leaderindex = nrand() % int64(len(ck.servers))
		}
		ok := ck.servers[leaderindex].Call("KVServer.PutAppend", putappendargs, putappendreply)
		if !ok || putappendreply.Err == ErrWrongLeader {
			leaderindex = (leaderindex + 1) % int64(len(ck.servers))
			DPrintf("Clerk[%v] PutAppend Fail, change leader : args - %+v reply - %+v leaderindex - %v ok - %v",
				key, putappendargs, putappendreply, leaderindex, ok)
			continue
		}
		// 可能还没有设定clerk的leader，设定它
		DPrintf("Clerk[%v] PutAppend OK : args - %+v reply - %+v leaderindex - %v ok - %v",
			key, putappendargs, putappendreply, leaderindex, ok)
		atomic.StoreInt64(&ck.leader, leaderindex)
		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

/*
	客户端的Put/Get都是等待执行完毕才返回并执行下一个的 - 从test_test.go来看。
	考虑到实际应用中，并发的访问应该不会等待另外的Put/Get回复。
	如果不等待的话，那server端的clientid2maxseqid的作用是不是有问题？因为多个并发访问，可能后面一个成功了，前面那个失败了啊？
	按照当前的逻辑，如果前面那个失败了，则它应该不可能在apply到kv.database了吧？
*/
