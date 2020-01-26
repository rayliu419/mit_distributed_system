package mapreduce

import (
	"encoding/json"
	"log"
	"os"
	"sort"
)

/*
	参数nMap需要的原因是：
		每个Mapper会给一个Reducer产生一个输入文件。例如4个Mapper，2个Reducer。
		第0个Mapper产生mapper0-0 mapper0-1 给Reducer0和Reducer1。
		第1个Mapper产生mapper1-0 mapper1-1 给Reducer0和Reducer1。
		所以Reducer0要处理mapper0-0, mapper1-0, mapper2-0, mapper3-0。最初我以为4个Mapper为同一个Reducer
		写同一个文件。
 */
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int,       // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//
	var key2valuelist map[string][]string
	key2valuelist = make(map[string][]string)
	for i := 0; i < nMap; i++ {
		fileName := reduceName(jobName, i, reduceTask)
		fp, err := os.Open(fileName)
		if err != nil {
			log.Fatal("doReduce: open intermediate file ", fileName, " error: ", err)
		}
		dec := json.NewDecoder(fp)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				break
			}
			_, ok := key2valuelist[kv.Key]
			if !ok {
				key2valuelist[kv.Key] = make([]string, 0)
			}
			key2valuelist[kv.Key] = append(key2valuelist[kv.Key], kv.Value)
		}
		fp.Close()
	}
	var keys []string
	for k, _ := range key2valuelist {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	mergeMapFile, err := os.Create(outFile)
	if err != nil {
		Debug("create mergeMapFile fail")
	}
	defer mergeMapFile.Close()
	enc := json.NewEncoder(mergeMapFile)
	for _, key := range keys {
		k := key
		reduceValue := reduceF(k, key2valuelist[k])
		err := enc.Encode(&KeyValue{k, reduceValue})
		if err != nil {
			Debug("encode %s-%v error", k, key2valuelist[k])
		}
	}
}
