package mapreduce

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
)

type sortedKV []KeyValue

func (kv sortedKV) Len() int {
	return len(kv)
}

func (kv sortedKV) Less(i, j int) bool {
	return kv[i].Key < kv[j].Key
}

func (kv sortedKV) Swap(i, j int) {
	kv[i], kv[j] = kv[j], kv[i]
}

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//生成需要读取的临时文件名
	//infiles := make([]string, nMap)
	var infiles []string
	fmt.Printf("task num : %d\n", nMap)
	for index := 0; index < nMap; index++ {
		name := reduceName(jobName, index, reduceTaskNumber)
		fmt.Println("filename:" + name)
		infiles = append(infiles, name)
		//infiles[index] = name
	}

	allMap := make(map[string][]string)
	var keys []string
	for _, file := range infiles {
		var arr []KeyValue
		data, err := ioutil.ReadFile(file)
		if err != nil {
			panic(err)
		}

		if err := json.Unmarshal(data, &arr); err != nil {
			panic(err)
		}
		//totalArr = append(totalArr, arr...)
		for _, kv := range arr {
			//keys = append(keys, kv.Key)
			allMap[kv.Key] = append(allMap[kv.Key], kv.Value)
		}
		//os.Remove(file)

	}

	for key := range allMap {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	outputFile := mergeName(jobName, reduceTaskNumber)
	output, err := os.OpenFile(outputFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		panic(err)
	}
	enc := json.NewEncoder(output)
	for _, key := range keys {
		result := reduceF(key, allMap[key])
		//ioutil.WriteFile(mergeName(jobName, reduceTaskNumber), []byte(result), 0666)
		//output.WriteString(result + "\n")
		kv := KeyValue{key, result}
		if err := enc.Encode(&kv); err != nil {
			panic(err)
		}
	}
	output.Close()
	// TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
}
