package raft

//
// Raft tests.
//
// we will use the original test_test.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import (
	"fmt"
	"testing"
)
import "time"
import "math/rand"
import "sync/atomic"
import "sync"

// The tester generously allows solutions to complete elections in one second
// (much more than the paper's range of timeouts).
const RaftElectionTimeout = 1000 * time.Millisecond

/*
	最基本的场景。判断是否有leader被选举出来。
 */
func TestInitialElection2A(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	cfg.begin("Test (2A): initial election")

	// is a leader elected?
	cfg.checkOneLeader()

	// sleep a bit to avoid racing with followers learning of the
	// election, then check that all peers agree on the term.
	time.Sleep(50 * time.Millisecond)
	term1 := cfg.checkTerms()

	// does the leader+term stay the same if there is no network failure?
	time.Sleep(2 * RaftElectionTimeout)
	term2 := cfg.checkTerms()
	if term1 != term2 {
		fmt.Printf("warning: term changed even though there were no failures")
	}

	// there should still be a leader.
	cfg.checkOneLeader()

	cfg.end()
}

/*
	这个场景是初始3个raft node正常工作。某个node变成了leader，然后发生了网络隔离，它的包发不出去，它的回复别的node也收不到。
	过了一段时间，这个node回到了集群，它应该迅速的知道自己不再是leader了。
	每个term要保证只有一个leader。
 */
func TestReElection2A(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	cfg.begin("Test (2A): election after network failure")

	leader1 := cfg.checkOneLeader()

	// if the leader disconnects, a new one should be elected.
	cfg.disconnect(leader1)
	cfg.checkOneLeader()

	// if the old leader rejoins, that shouldn't
	// disturb the new leader.
	cfg.connect(leader1)
	leader2 := cfg.checkOneLeader()

	// if there's no quorum, no leader should
	// be elected.
	cfg.disconnect(leader2)
	cfg.disconnect((leader2 + 1) % servers)
	time.Sleep(2 * RaftElectionTimeout)
	cfg.checkNoLeader()

	// if a quorum arises, it should elect a leader.
	cfg.connect((leader2 + 1) % servers)
	cfg.checkOneLeader()

	// re-join of last node shouldn't prevent leader from existing.
	cfg.connect(leader2)
	cfg.checkOneLeader()

	cfg.end()
}

/*
	这个场景是5个node提交一个log的场景。模拟client向集群提交一个command，只有leader接受command
	1.初始没选举出leader，所有都是follower，拒绝接受command。
	2.选举户一个leader以后，接受command，并且最后成功提交command。
	3.连续成功提交3个command。
 */
func TestBasicAgree2B(t *testing.T) {
	servers := 5
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B): basic agreement")

	iters := 3
	for index := 1; index < iters+1; index++ {
		nd, _ := cfg.nCommitted(index)
		if nd > 0 {
			t.Fatalf("some have committed before Start()")
		}

		xindex := cfg.one(index*100, servers, false)
		if xindex != index {
			t.Fatalf("got index %v but expected %v", xindex, index)
		}
	}

	cfg.end()
}

/*
	3个node挂了1个，还能继续commit日志。
	挂掉的node回来后，日志能跟上已经提交的日志。
*/
func TestFailAgree2B(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B): agreement despite follower disconnection")

	cfg.one(101, servers, false)

	// follower network disconnection
	leader := cfg.checkOneLeader()
	cfg.disconnect((leader + 1) % servers)

	// agree despite one disconnected server?
	cfg.one(102, servers-1, false)
	cfg.one(103, servers-1, false)
	time.Sleep(RaftElectionTimeout)
	cfg.one(104, servers-1, false)
	cfg.one(105, servers-1, false)

	// re-connect
	cfg.connect((leader + 1) % servers)

	// agree with full set of servers?
	cfg.one(106, servers, true)
	time.Sleep(RaftElectionTimeout)
	cfg.one(107, servers, true)

	cfg.end()
}

/*
	5个node，选为1个leader以后提交一次日志。
	断开其中的3个node，断开的3个node由于收不到心跳在不断的发起投票，但是无法成为新的leader。
	老的leader收到一个新command，但是投票不过半，所以提交不了。
	恢复网络以后，断开的3个node之一成为新leader，把老leader的command(20)清除掉。
*/
func TestFailNoAgree2B(t *testing.T) {
	servers := 5
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B): no agreement if too many followers disconnect")

	cfg.one(10, servers, false)
	// 3 of 5 followers disconnect
	leader := cfg.checkOneLeader()
	DPrintf("disconnect %v %v %v", (leader + 1) % servers, (leader + 2) % servers, (leader + 3) % servers)
	cfg.disconnect((leader + 1) % servers)
	cfg.disconnect((leader + 2) % servers)
	cfg.disconnect((leader + 3) % servers)
	// 在断开以后，实际上断开的3个节点之间也无法通信，导致有4个网络，只列举一个情况。
	// [(0,1) (2) (3) (4)]，断开前，0是leader。

	// 20这个命令提交不了，因为(0,1) 不够大多数，但是log还是被写到了0和1的日志中。
	index, _, ok := cfg.rafts[leader].Start(20)
	if ok != true {
		t.Fatalf("leader rejected Start()")
	}
	if index != 2 {
		t.Fatalf("expected index 2, got %v", index)
	}

	time.Sleep(2 * RaftElectionTimeout)

	n, _ := cfg.nCommitted(index)
	if n > 0 {
		t.Fatalf("%v committed but no majority", n)
	}

	// repair
	DPrintf("reconnect %v %v %v", (leader + 1) % servers, (leader + 2) % servers, (leader + 3) % servers)
	cfg.connect((leader + 1) % servers)
	cfg.connect((leader + 2) % servers)
	cfg.connect((leader + 3) % servers)
	// 加入后重新选主
	/*
		图2中并没说有说follower比leader日志长怎么办，只说如果conflict怎么处理，不同的log index比较看起来没意义。
		这个case挂掉的原因在于:
		0,1含有两个log entry (2, 10) (2, 20)
		2,3,4只有一个log entry (2, 10)
		在2重选为leader以后，日志变为(2, 10), (16, 30), (16, 1000)
		appendEntries()的处理，现在没有截断0,1的日志。(2, 20)没有commit，应该是无法comit，截断时应该删掉才对。
	 */
	// the disconnected majority may have chosen a leader from
	// among their own ranks, forgetting index 2.
	leader2 := cfg.checkOneLeader()
	index2, _, ok2 := cfg.rafts[leader2].Start(30)
	if ok2 == false {
		t.Fatalf("leader2 rejected Start()")
	}
	if index2 < 2 || index2 > 3 {
		t.Fatalf("unexpected index %v", index2)
	}

	cfg.one(1000, servers, true)

	cfg.end()
}

/*
	这个case是在短时间内大量的发日志，然后确定这些日志都被提交了。
*/
func TestConcurrentStarts2B(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B): concurrent Start()s")

	var success bool
loop:
	for try := 0; try < 5; try++ {
		if try > 0 {
			// give solution some time to settle
			time.Sleep(3 * time.Second)
		}

		leader := cfg.checkOneLeader()
		_, term, ok := cfg.rafts[leader].Start(1)
		if !ok {
			// leader moved on really quickly
			continue
		}

		iters := 5
		var wg sync.WaitGroup
		is := make(chan int, iters)
		for ii := 0; ii < iters; ii++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				i, term1, ok := cfg.rafts[leader].Start(100 + i)
				if term1 != term {
					return
				}
				if ok != true {
					return
				}
				is <- i
			}(ii)
		}

		wg.Wait()
		close(is)

		for j := 0; j < servers; j++ {
			if t, _ := cfg.rafts[j].GetState(); t != term {
				// term changed -- can't expect low RPC counts
				continue loop
			}
		}

		failed := false
		cmds := []int{}
		for index := range is {
			cmd := cfg.wait(index, servers, term)
			if ix, ok := cmd.(int); ok {
				if ix == -1 {
					// peers have moved on to later terms
					// so we can't expect all Start()s to
					// have succeeded
					failed = true
					break
				}
				cmds = append(cmds, ix)
			} else {
				t.Fatalf("value %v is not an int", cmd)
			}
		}

		if failed {
			// avoid leaking goroutines
			go func() {
				for range is {
				}
			}()
			continue
		}

		for ii := 0; ii < iters; ii++ {
			x := 100 + ii
			ok := false
			for j := 0; j < len(cmds); j++ {
				if cmds[j] == x {
					ok = true
				}
			}
			if ok == false {
				t.Fatalf("cmd %v missing in %v", x, cmds)
			}
		}

		success = true
		break
	}

	if !success {
		t.Fatalf("term changed too often")
	}

	cfg.end()
}

/*
	这个case主要测试的是old leader加入回去以后，不能满足再次被选举成leader，且它之前接受的没有commit的log entry能被
	它脱离出去之后提交的log entry给删除。
*/
func TestRejoin2B(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B): rejoin of partitioned leader")

	cfg.one(101, servers, true)
	// (101) 被提交

	// leader network failure
	leader1 := cfg.checkOneLeader()
	cfg.disconnect(leader1)
	DPrintf("config.go disconnect leader %v", leader1)

	// make old leader try to agree on some entries
	cfg.rafts[leader1].Start(102)
	cfg.rafts[leader1].Start(103)
	cfg.rafts[leader1].Start(104)
	// 老leader有3个日志102，103，104

	// new leader commits, also for index=2
	cfg.one(103, 2, true)

	// new leader network failure
	leader2 := cfg.checkOneLeader()
	cfg.disconnect(leader2)
	DPrintf("config.go disconnect leader %v", leader2)

	// old leader connected again
	cfg.connect(leader1)
	DPrintf("config.go leader %v reconnect", leader1)

	cfg.one(104, 2, true)

	// all together now
	cfg.connect(leader2)

	cfg.one(105, servers, true)

	cfg.end()
}

/*
	这个case是来回测试leader出去又回来的情况。
 */
func TestBackup2B(t *testing.T) {
	servers := 5
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B): leader backs up quickly over incorrect follower logs")

	cfg.one(rand.Int(), servers, true)

	// put leader and one follower in a partition
	leader1 := cfg.checkOneLeader()
	cfg.disconnect((leader1 + 2) % servers)
	cfg.disconnect((leader1 + 3) % servers)
	cfg.disconnect((leader1 + 4) % servers)
	DPrintf("config.go disconnect %v", (leader1 + 2) % servers)
	DPrintf("config.go disconnect %v", (leader1 + 3) % servers)
	DPrintf("config.go disconnect %v", (leader1 + 4) % servers)

	// submit lots of commands that won't commit
	for i := 0; i < 50; i++ {
		cfg.rafts[leader1].Start(rand.Int())
	}

	time.Sleep(RaftElectionTimeout / 2)

	cfg.disconnect((leader1 + 0) % servers)
	cfg.disconnect((leader1 + 1) % servers)
	DPrintf("config.go disconnect %v", (leader1 + 0) % servers)
	DPrintf("config.go disconnect %v", (leader1 + 1) % servers)

	// allow other partition to recover
	cfg.connect((leader1 + 2) % servers)
	cfg.connect((leader1 + 3) % servers)
	cfg.connect((leader1 + 4) % servers)
	DPrintf("config.go reconnect %v", (leader1 + 2) % servers)
	DPrintf("config.go reconnect %v", (leader1 + 3) % servers)
	DPrintf("config.go reconnect %v", (leader1 + 4) % servers)

	// lots of successful commands to new group.
	for i := 0; i < 50; i++ {
		cfg.one(rand.Int(), 3, true)
	}

	// now another partitioned leader and one follower
	leader2 := cfg.checkOneLeader()
	other := (leader1 + 2) % servers
	if leader2 == other {
		other = (leader2 + 1) % servers
	}
	cfg.disconnect(other)

	// lots more commands that won't commit
	for i := 0; i < 50; i++ {
		cfg.rafts[leader2].Start(rand.Int())
	}

	time.Sleep(RaftElectionTimeout / 2)

	// bring original leader back to life,
	for i := 0; i < servers; i++ {
		cfg.disconnect(i)
	}
	cfg.connect((leader1 + 0) % servers)
	cfg.connect((leader1 + 1) % servers)
	cfg.connect(other)

	// lots of successful commands to new group.
	for i := 0; i < 50; i++ {
		cfg.one(rand.Int(), 3, true)
	}

	// now everyone
	for i := 0; i < servers; i++ {
		cfg.connect(i)
	}
	cfg.one(rand.Int(), servers, true)

	cfg.end()
}

func TestCount2B(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B): RPC counts aren't too high")

	rpcs := func() (n int) {
		for j := 0; j < servers; j++ {
			n += cfg.rpcCount(j)
		}
		return
	}

	leader := cfg.checkOneLeader()

	total1 := rpcs()

	if total1 > 30 || total1 < 1 {
		t.Fatalf("too many or few RPCs (%v) to elect initial leader\n", total1)
	}

	var total2 int
	var success bool
loop:
	for try := 0; try < 5; try++ {
		if try > 0 {
			// give solution some time to settle
			time.Sleep(3 * time.Second)
		}

		leader = cfg.checkOneLeader()
		total1 = rpcs()

		iters := 10
		starti, term, ok := cfg.rafts[leader].Start(1)
		if !ok {
			// leader moved on really quickly
			continue
		}
		cmds := []int{}
		for i := 1; i < iters+2; i++ {
			x := int(rand.Int31())
			cmds = append(cmds, x)
			index1, term1, ok := cfg.rafts[leader].Start(x)
			if term1 != term {
				// Term changed while starting
				continue loop
			}
			if !ok {
				// No longer the leader, so term has changed
				continue loop
			}
			if starti+i != index1 {
				t.Fatalf("Start() failed")
			}
		}

		for i := 1; i < iters+1; i++ {
			cmd := cfg.wait(starti+i, servers, term)
			if ix, ok := cmd.(int); ok == false || ix != cmds[i-1] {
				if ix == -1 {
					// term changed -- try again
					continue loop
				}
				t.Fatalf("wrong value %v committed for index %v; expected %v\n", cmd, starti+i, cmds)
			}
		}

		failed := false
		total2 = 0
		for j := 0; j < servers; j++ {
			if t, _ := cfg.rafts[j].GetState(); t != term {
				// term changed -- can't expect low RPC counts
				// need to keep going to update total2
				failed = true
			}
			total2 += cfg.rpcCount(j)
		}

		if failed {
			continue loop
		}

		if total2-total1 > (iters+1+3)*3 {
			t.Fatalf("too many RPCs (%v) for %v entries\n", total2-total1, iters)
		}

		success = true
		break
	}

	if !success {
		t.Fatalf("term changed too often")
	}

	time.Sleep(RaftElectionTimeout)

	total3 := 0
	for j := 0; j < servers; j++ {
		total3 += cfg.rpcCount(j)
	}

	if total3-total2 > 3*20 {
		t.Fatalf("too many RPCs (%v) for 1 second of idleness\n", total3-total2)
	}

	cfg.end()
}
/*
	persist的核心是：
		reply RPC之前要持久化。
		修改log, votefor, term要持久化。这个Start()要持久化容易漏掉。
 */


func TestPersist12C(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	cfg.begin("Test (2C): basic persistence")

	cfg.one(11, servers, true)

	// crash and re-start all
	for i := 0; i < servers; i++ {
		cfg.start1(i)
	}
	for i := 0; i < servers; i++ {
		cfg.disconnect(i)
		cfg.connect(i)
	}

	cfg.one(12, servers, true)

	leader1 := cfg.checkOneLeader()
	cfg.disconnect(leader1)
	cfg.start1(leader1)
	cfg.connect(leader1)

	cfg.one(13, servers, true)

	leader2 := cfg.checkOneLeader()
	cfg.disconnect(leader2)
	cfg.one(14, servers-1, true)
	cfg.start1(leader2)
	cfg.connect(leader2)

	cfg.wait(4, servers, -1) // wait for leader2 to join before killing i3

	i3 := (cfg.checkOneLeader() + 1) % servers
	cfg.disconnect(i3)
	cfg.one(15, servers-1, true)
	cfg.start1(i3)
	cfg.connect(i3)

	cfg.one(16, servers, true)

	cfg.end()
}

func TestPersist22C(t *testing.T) {
	servers := 5
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	cfg.begin("Test (2C): more persistence")

	index := 1
	for iters := 0; iters < 5; iters++ {
		cfg.one(10+index, servers, true)
		index++

		leader1 := cfg.checkOneLeader()

		cfg.disconnect((leader1 + 1) % servers)
		cfg.disconnect((leader1 + 2) % servers)

		cfg.one(10+index, servers-2, true)
		index++

		cfg.disconnect((leader1 + 0) % servers)
		cfg.disconnect((leader1 + 3) % servers)
		cfg.disconnect((leader1 + 4) % servers)

		cfg.start1((leader1 + 1) % servers)
		cfg.start1((leader1 + 2) % servers)
		cfg.connect((leader1 + 1) % servers)
		cfg.connect((leader1 + 2) % servers)

		time.Sleep(RaftElectionTimeout)

		cfg.start1((leader1 + 3) % servers)
		cfg.connect((leader1 + 3) % servers)

		cfg.one(10+index, servers-2, true)
		index++

		cfg.connect((leader1 + 4) % servers)
		cfg.connect((leader1 + 0) % servers)
	}

	cfg.one(1000, servers, true)

	cfg.end()
}

func TestPersist32C(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	cfg.begin("Test (2C): partitioned leader and one follower crash, leader restarts")

	cfg.one(101, 3, true)
	// 假设leader1 = 0, 101被node 0 提交

	leader := cfg.checkOneLeader()
	cfg.disconnect((leader + 2) % servers)
	// node 2(follower)断开连接，由于它无法接到心跳，它会不断的提高自己的term并且发起投票

	cfg.one(102, 2, true)
	// 102 被剩下的两台node 0 和node 1 提交

	cfg.crash1((leader + 0) % servers)
	cfg.crash1((leader + 1) % servers)
	// node 0 和 node 1挂了，但是他们的log有两条提交的命令(101)(102)

	cfg.connect((leader + 2) % servers)
	// node 2 恢复

	cfg.start1((leader + 0) % servers)
	cfg.connect((leader + 0) % servers)
	// node 0 恢复，与node 2成为一个网络。虽然node 2的term大，但是它无法获得node 0 的投票，因为它的日志短了。
	// node 0 经过requestVote的处理，Term赶了上来，并且开始发起它的投票并获取leader地位。

	cfg.one(103, 2, true)
	// node 0 接受了(103)的提交。

	cfg.start1((leader + 1) % servers)
	cfg.connect((leader + 1) % servers)
	// node 1重新连接，它也获取不了leader地位，原因跟node 2前面一样。

	cfg.one(104, servers, true)
	// 最后3个node提交(104)

	cfg.end()
}

//
// Test the scenarios described in Figure 8 of the extended Raft paper. Each
// iteration asks a leader, if there is one, to insert a command in the Raft
// log.  If there is a leader, that leader will fail quickly with a high
// probability (perhaps without committing the command), or crash after a while
// with low probability (most likey committing the command).  If the number of
// alive servers isn't enough to form a majority, perhaps start a new server.
// The leader in a new term may try to finish replicating log entries that
// haven't been committed yet.
//
/*
	paper的图8的问题序列 - 对照paper来看。
	1. 5个node。node1(term=2)的情况下是leader，写入了一个日志(term=2, index=2, cmd=2)，复制给了node2，然后crash了。
	2. 由于没有leader了，大家重新选举。node5(term=3)当选leader，并且写入了一个日志(term=3, index=2, cmd=3)。
	3. node5挂了，由于没有leader，大家又重新选举。node1(term=4)回来了，并且当选leader。它把日志(term=2, index=2, cmd=2)复制给了node3，
	接到回复以后，按照超过一半的规则，(term=2, index=2, cmd=2)这条日志被commit了。同时接到了新请求，写入了新日志(term=4, index=3, cmd=4)，
	在这条日志复制给其他node之前，它又挂了。
	4. node5又回来了，它发起投票并当选leader。(term=5)
	5. node5开始复制日志(term=3, index=2, cmd=3)，node1-node4的(term=2, index=2, cmd=2)都被node5的(term=3, index=2, cmd=3)
	覆盖了，但是(term=2, index=2, cmd=2)已经被commit，实际上是不应该被覆盖的。
	6. 图8(e)表示，如果(term=4, index=3, cmd=4)被复制到了大多数，第4步的node5获取不了leader地位，因为它的最新日志是(term=3, index=2, cmd=3)
	，老于日志(term=4, index=3, cmd=4)
	这个流程说明了，leader对于非任期的日志项，不能靠counting多数来commit。基于这个case，commit不再认为多半数的node存取日志则认为
	此日志项commit，需要加上前提条件 - 对于本任期的，才能采取counting多数来提交，同时根据log match property，如果某个index被提交，则其前面
	也别提交。对于这个case，在第3步，(term=2, index=2, cmd=2)不能被提交，因为leader是term=4，只有在第3步的(term=4, index=3, cmd=4)
	被复制了，(term=2, index=2, cmd=2)才一起被提交了。
 */
func TestFigure82C(t *testing.T) {
	servers := 5
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	cfg.begin("Test (2C): Figure 8")

	cfg.one(rand.Int(), 1, true)

	nup := servers
	for iters := 0; iters < 1000; iters++ {
		leader := -1
		for i := 0; i < servers; i++ {
			if cfg.rafts[i] != nil {
				_, _, ok := cfg.rafts[i].Start(rand.Int())
				if ok {
					leader = i
				}
			}
		}

		if (rand.Int() % 1000) < 100 {
			ms := rand.Int63() % (int64(RaftElectionTimeout/time.Millisecond) / 2)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		} else {
			ms := (rand.Int63() % 13)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}

		if leader != -1 {
			cfg.crash1(leader)
			nup -= 1
		}

		if nup < 3 {
			s := rand.Int() % servers
			if cfg.rafts[s] == nil {
				cfg.start1(s)
				cfg.connect(s)
				nup += 1
			}
		}
	}

	for i := 0; i < servers; i++ {
		if cfg.rafts[i] == nil {
			cfg.start1(i)
			cfg.connect(i)
		}
	}

	cfg.one(rand.Int(), servers, true)

	cfg.end()
}

func TestUnreliableAgree2C(t *testing.T) {
	servers := 5
	cfg := make_config(t, servers, true)
	defer cfg.cleanup()

	cfg.begin("Test (2C): unreliable agreement")

	var wg sync.WaitGroup

	for iters := 1; iters < 50; iters++ {
		for j := 0; j < 4; j++ {
			wg.Add(1)
			go func(iters, j int) {
				defer wg.Done()
				cfg.one((100*iters)+j, 1, true)
			}(iters, j)
		}
		cfg.one(iters, 1, true)
	}

	cfg.setunreliable(false)

	wg.Wait()

	cfg.one(100, servers, true)

	cfg.end()
}

/*
	TestFigure8Unreliable2C 这测试用例一共运行不超过40s。前10秒在可靠网络各种插入数据。
	中间20秒，在不可靠网络中各种插入数据。最后10s，开始的时候，恢复网络，插入一个数据，等待这个数据被提交，如果超过这10s就报错。
	通过减少了心跳间隔时间来解决了这个问题。
	心跳变慢：日志复制的速度变慢，有可能导致最后10s内无法复制完成。
 */
func TestFigure8Unreliable2C(t *testing.T) {
	servers := 5
	cfg := make_config(t, servers, true)
	defer cfg.cleanup()

	cfg.begin("Test (2C): Figure 8 (unreliable)")

	cfg.one(rand.Int()%10000, 1, true)

	nup := servers
	for iters := 0; iters < 1000; iters++ {
		if iters == 200 {
			cfg.setlongreordering(true)
		}
		leader := -1
		for i := 0; i < servers; i++ {
			_, _, ok := cfg.rafts[i].Start(rand.Int() % 10000)
			if ok && cfg.connected[i] {
				leader = i
			}
		}

		if (rand.Int() % 1000) < 100 {
			ms := rand.Int63() % (int64(RaftElectionTimeout/time.Millisecond) / 2)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		} else {
			ms := (rand.Int63() % 13)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}

		if leader != -1 && (rand.Int()%1000) < int(RaftElectionTimeout/time.Millisecond)/2 {
			cfg.disconnect(leader)
			nup -= 1
		}

		if nup < 3 {
			s := rand.Int() % servers
			if cfg.connected[s] == false {
				cfg.connect(s)
				nup += 1
			}
		}
	}

	for i := 0; i < servers; i++ {
		if cfg.connected[i] == false {
			cfg.connect(i)
		}
	}

	cfg.one(rand.Int()%10000, servers, true)

	cfg.end()
}

func internalChurn(t *testing.T, unreliable bool) {

	servers := 5
	cfg := make_config(t, servers, unreliable)
	defer cfg.cleanup()

	if unreliable {
		cfg.begin("Test (2C): unreliable churn")
	} else {
		cfg.begin("Test (2C): churn")
	}

	stop := int32(0)

	// create concurrent clients
	cfn := func(me int, ch chan []int) {
		var ret []int
		ret = nil
		defer func() { ch <- ret }()
		values := []int{}
		for atomic.LoadInt32(&stop) == 0 {
			x := rand.Int()
			index := -1
			ok := false
			for i := 0; i < servers; i++ {
				// try them all, maybe one of them is a leader
				cfg.mu.Lock()
				rf := cfg.rafts[i]
				cfg.mu.Unlock()
				if rf != nil {
					index1, _, ok1 := rf.Start(x)
					if ok1 {
						ok = ok1
						index = index1
					}
				}
			}
			if ok {
				// maybe leader will commit our value, maybe not.
				// but don't wait forever.
				for _, to := range []int{10, 20, 50, 100, 200} {
					nd, cmd := cfg.nCommitted(index)
					if nd > 0 {
						if xx, ok := cmd.(int); ok {
							if xx == x {
								values = append(values, x)
							}
						} else {
							cfg.t.Fatalf("wrong command type")
						}
						break
					}
					time.Sleep(time.Duration(to) * time.Millisecond)
				}
			} else {
				time.Sleep(time.Duration(79+me*17) * time.Millisecond)
			}
		}
		ret = values
	}

	ncli := 3
	cha := []chan []int{}
	for i := 0; i < ncli; i++ {
		cha = append(cha, make(chan []int))
		go cfn(i, cha[i])
	}

	for iters := 0; iters < 20; iters++ {
		if (rand.Int() % 1000) < 200 {
			i := rand.Int() % servers
			cfg.disconnect(i)
		}

		if (rand.Int() % 1000) < 500 {
			i := rand.Int() % servers
			if cfg.rafts[i] == nil {
				cfg.start1(i)
			}
			cfg.connect(i)
		}

		if (rand.Int() % 1000) < 200 {
			i := rand.Int() % servers
			if cfg.rafts[i] != nil {
				cfg.crash1(i)
			}
		}

		// Make crash/restart infrequent enough that the peers can often
		// keep up, but not so infrequent that everything has settled
		// down from one change to the next. Pick a value smaller than
		// the election timeout, but not hugely smaller.
		time.Sleep((RaftElectionTimeout * 7) / 10)
	}

	time.Sleep(RaftElectionTimeout)
	cfg.setunreliable(false)
	for i := 0; i < servers; i++ {
		if cfg.rafts[i] == nil {
			cfg.start1(i)
		}
		cfg.connect(i)
	}

	atomic.StoreInt32(&stop, 1)

	values := []int{}
	for i := 0; i < ncli; i++ {
		vv := <-cha[i]
		if vv == nil {
			t.Fatal("client failed")
		}
		values = append(values, vv...)
	}

	time.Sleep(RaftElectionTimeout)

	lastIndex := cfg.one(rand.Int(), servers, true)

	really := make([]int, lastIndex+1)
	for index := 1; index <= lastIndex; index++ {
		v := cfg.wait(index, servers, -1)
		if vi, ok := v.(int); ok {
			really = append(really, vi)
		} else {
			t.Fatalf("not an int")
		}
	}

	for _, v1 := range values {
		ok := false
		for _, v2 := range really {
			if v1 == v2 {
				ok = true
			}
		}
		if ok == false {
			cfg.t.Fatalf("didn't find a value")
		}
	}

	cfg.end()
}

func TestReliableChurn2C(t *testing.T) {
	internalChurn(t, false)
}

func TestUnreliableChurn2C(t *testing.T) {
	internalChurn(t, true)
}
