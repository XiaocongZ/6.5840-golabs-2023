package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"
import "os"
import "path/filepath"
import "strings"
import "sort"

//The worker should put intermediate Map output in files in the current directory, where your worker can later read them as input to Reduce tasks.
//Each mapper should create nReduce intermediate files for consumption by the reduce tasks.
//naming convention for intermediate Map output: intermediate_{inputfile name string}_{partition number}

//TODO Do I consider mappers crash, losing intermediate outputs?
type worker struct {
	state int
	timeStamp time.Time
	mapf func(string, string) []KeyValue
	reducef func(string, []string) string
}


const (
	nullWorker int = iota //not allocated
	idle
	in_progress
	completed
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()
	//os.Exit(0)
	for {
		reply := CallRequestTask()
		//fmt.Println(reply)
		if reply.Exit {
			return
		} else if reply.IsMapTask {
			//mapf on input
			fileByte, err := os.ReadFile(reply.Files[0])
		   	if err != nil {
		      	fmt.Println("cannot read inputfile ", reply.Files[0])
				os.Exit(1)
		   	}
			intermediateKVs := mapf(reply.Files[0], string(fileByte))
			//partition in nReduce
			intermediateArray := make([]string, reply.ReduceN)
			for _, item := range intermediateKVs {
				intermediateArray[ihash(item.Key) % reply.ReduceN] += item.Key + " " + item.Value + "\n"
			}
			for i := 0; i < reply.ReduceN; i++ {
				intermediateFilename := toIntermediateFilename(reply.Files[0])
				filenameString := fmt.Sprintf("%s_%d", intermediateFilename, i)
				intermediateArray[i] = strings.TrimSuffix(intermediateArray[i], "\n")
				os.WriteFile(filenameString, []byte(intermediateArray[i]), 0777)
			}
			//notify finish
			CallFinish(FinishArgs{false, true, false, reply.Files, 0})

		} else if reply.IsReduceTask {
			/*
			get all intermediate Key Value
			sort Key
			for Key do:
				get all Value
				put all Value in reducef()
				append res to mr-out-N
			*/
			intermediateArray := make([]string, 0)
			for _, file := range reply.Files {
				intermediateFilename := toIntermediateFilename(file)
				filenameString := fmt.Sprintf("%s_%d", intermediateFilename, reply.ReduceN)

				fileByte, err := os.ReadFile(filenameString)
			   	if err != nil {
			      	fmt.Println("cannot read inputfile ", filenameString)
					os.Exit(1)
			   	}
				fileString := string(fileByte)
				intermediateArray = append(intermediateArray, strings.Split(fileString, "\n")...)
			}
			//sort intermediateArray
			sort.Slice(intermediateArray, func(i, j int) bool { return strings.Split(intermediateArray[i], " ")[0] < strings.Split(intermediateArray[j], " ")[0] })
			mr_outString := ""
			key := ""
			var start int
			for i, keyValue := range intermediateArray {
				if keyValue == "" {
					//fmt.Println("a null keyValue")
					continue
				}
				currentKey, currentValue := getKeyValue(keyValue)
				if key == "" {
					key = currentKey
					start = i
					//strip keys to feed only values to reducef
					intermediateArray[i] = currentValue
				} else if key != currentKey {
					//feed to reducef; start a new key
					mr_outString += key + " " + reducef(key, intermediateArray[start: i]) + "\n"
					key = currentKey
					start = i
					intermediateArray[i] = currentValue
				} else { // key == currentKey
					intermediateArray[i] = currentValue
				}
			}
			mr_outString += key + " " + reducef(key, intermediateArray[start: ])
			mr_outString = strings.TrimSuffix(mr_outString, "\n")
			filenameString := fmt.Sprintf("mr-out-%d", reply.ReduceN)

			os.WriteFile(filenameString, []byte(mr_outString), 0777)
			CallFinish(FinishArgs{false, false, true, nil, reply.ReduceN})
		} else {
			time.Sleep(time.Second)
		}
	}
}

func getKeyValue(keyValue string) (string, string) {
	if keyValue == "" {
		fmt.Println("null keyvalue")
		os.Exit(2)
	}
	return strings.Split(keyValue, " ")[0], strings.Split(keyValue, " ")[1]
}

func toIntermediateFilename(inputFilename string) string {
	dir := filepath.Dir(inputFilename)
	intermediateFilename := filepath.Join( dir, "intermediate_" + filepath.Base(inputFilename) )
	return intermediateFilename
}

func CallRequestTask() RequestTaskReply{
	args := NullArgs{}
	reply := RequestTaskReply{}
	ok := call("Coordinator.RequestTask", &args, &reply)
	if ok {
		//fmt.Println("ok", reply)
		return reply
	} else {
		return RequestTaskReply{true, false, false, nil, 0}
	}
}

func CallFinish(args FinishArgs) {
	reply := NullArgs{}
	ok := call("Coordinator.Finish", &args, &reply)
	if ok {
		//fmt.Println("CallFinish: ok")
	} else {
		fmt.Println("CallFinish: error")
		return
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Println("reply.Y: ")
		fmt.Println(reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
