package mapreduce

import (
	"fmt"
	"sync"
	"time"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	var taskcompletemutex sync.Mutex
	var taskstarttimemutex sync.Mutex
	// 记录某个task的完成情况
	taskcomplete := make(map[int]bool)
	// 记录某个task的启动时间
	taskstarttime := make(map[int]time.Time)
	taskcompletemutex.Lock()
	for i := 0; i < ntasks; i++ {
		taskcomplete[i] = false
	}
	taskcompletemutex.Unlock()
	for {
		worker := <-registerChan
		taskindex := findTaskIndex(taskcomplete, &taskcompletemutex, taskstarttime, &taskstarttimemutex)
		for {
			if taskindex == -1 && !completeAllTask(taskcomplete, &taskcompletemutex){
				fmt.Printf("Schedule: can'g find a task to run, sleep for 500 ms\n")
				time.Sleep(2*time.Second)
				taskindex = findTaskIndex(taskcomplete, &taskcompletemutex, taskstarttime, &taskstarttimemutex)
			} else {
				break
			}
		}
		if completeAllTask(taskcomplete, &taskcompletemutex) {
			break
		}
		dotaskargs := &DoTaskArgs{jobName, mapFiles[taskindex], phase, taskindex, n_other}
		taskstarttimemutex.Lock()
		taskstarttime[taskindex] = time.Now()
		taskstarttimemutex.Unlock()
		go assignTask(worker, dotaskargs, registerChan, taskcomplete, &taskcompletemutex)
	}
	fmt.Printf("Schedule: %v done\n", phase)
}

func assignTask(worker string, arg *DoTaskArgs, registerChan chan string,
	taskcomplete map[int]bool, mutex *sync.Mutex) {
	ok := call(worker, "Worker.DoTask", arg, nil)
	if ok == false {
		fmt.Printf("assign task fails worker - %s DoTaskArgs - %+v\n", worker, arg)
	} else {
		fmt.Printf("complete task worker - %s DoTaskArgs - %+v\n", worker, arg)
		mutex.Lock()
		taskcomplete[arg.TaskNumber] = true
		mutex.Unlock()
		// 执行完毕的worker可以重新被使用
		registerChan <- worker
	}
}

func findTaskIndex(taskcomplete map[int]bool, taskcompletemutex *sync.Mutex, taskstarttime map[int]time.Time, taskstartimemutex *sync.Mutex) int {
	taskcompletemutex.Lock()
	taskstartimemutex.Lock()
	defer taskstartimemutex.Unlock()
	defer taskcompletemutex.Unlock()
	for taskindex, status := range taskcomplete {
		if status == false && time.Since(taskstarttime[taskindex]) > 10*time.Second {
			return taskindex
		}
	}
	return -1
}

func completeAllTask(taskcomplete map[int]bool, mutex *sync.Mutex) bool {
	mutex.Lock()
	defer mutex.Unlock()
	for _, status := range taskcomplete {
		if status == false {
			return false
		}
	}
	return true
}
