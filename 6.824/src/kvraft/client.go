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
	leader int
	clientid int64
	seqid int64
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
/*
	Get请求具体应该怎么处理才能达到强一致性？
 */
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	getargs := &GetArgs{}
	getreply := &GetReply{}
	getargs.Key = key
	leaderindex := ck.leader
	for {
		if leaderindex == -1 {
			// 如果没有记录leader
			leaderindex = int(nrand()) % len(ck.servers)
		}
		DPrintf("Clerk Get : args - %+v reply - %+v leaderindex - %v", getargs, getreply, leaderindex)
		ok := ck.servers[leaderindex].Call("KVServer.Get", getargs, getreply)
		if ok {
			if getreply.Err == ErrWrongLeader {
				// 请求失败，需要请求其他的server
				leaderindex = (leaderindex + 1) % len(ck.servers)
			} else if getreply.Err == OK {
				// 可能还没有设定clerk的leader，设定它
				ck.leader = leaderindex
				return getreply.Value
			} else {
				// ErrNoKey - 返回空串
				return ""
			}
		} else {
			// 请求失败，需要请求其他的server
			leaderindex = (leaderindex + 1) % len(ck.servers)
		}
	}
	return ""
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
	leaderindex := ck.leader
	for {
		if leaderindex == -1 {
			// 如果没有记录leader
			leaderindex = int(nrand()) % len(ck.servers)
		}
		DPrintf("Clerk PutAppend : args - %+v reply - %+v leaderindex - %v", putappendargs, putappendreply, leaderindex)
		ok := ck.servers[leaderindex].Call("KVServer.PutAppend", putappendargs, putappendreply)
		if ok {
			if putappendreply.Err == ErrWrongLeader {
				// 请求失败，需要请求其他的server
				leaderindex = (leaderindex + 1) % len(ck.servers)
				continue
			} else if putappendreply.Err == OK {
				// 可能还没有设定clerk的leader，设定它
				ck.leader = leaderindex
				return
			} else {
				// ErrNoKey - 暂时没想到怎么处理
			}
		} else {
			// 请求失败，需要请求其他的server
			leaderindex = (leaderindex + 1) % len(ck.servers)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
