package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "fmt"
import "time"

//The pg-*.txt arguments to mrcoordinator.go are the input files; each file corresponds to one "split", and is the input to one Map task.

type Task struct {
	//isMapTask bool
	state int
	timeStamp time.Time
}
//enum for Task.state
const (
	nullTask int = iota //not initialized
	dispatchable
	dispatched
	finished
)

type Coordinator struct {
	// Your definitions here.
	nReduce int
	files []string
	workerStates map[string]worker
	mapTasks map[string]Task
	reduceTasks map[int]Task
	isMapDone bool
	isDone bool
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	//reply.Y = args.X + 1
	reply.Y = []int{1, 2, 3}
	return nil
}

//register a worker at coordinator, return hash style ID
func (c *Coordinator) Register(replyWorkerID *string) error {
	return nil
}
//request work to do
func (c *Coordinator) RequestTask(args *NullArgs, reply *RequestTaskReply) error {
	if c.Done() {
		//tell worker to exit
		*reply = RequestTaskReply{true, false, false, nil, 0}
		return nil
	}
	for file, task := range c.mapTasks {
		if task.state == dispatchable {
			//TODO dispatch lock

			*reply = RequestTaskReply {false, true, false, []string{file}, c.nReduce}
			c.mapTasks[file] = Task{dispatched, time.Now()}
			return nil
			break
		}
	}
	//TODO
	if ! c.MapDone() {
		//tell worker to wait
		*reply = RequestTaskReply {false, false, false, nil, 0}
		return nil
	}
	for reduceN, task := range c.reduceTasks {
		if task.state == dispatchable {
			//TODO dispatch lock
			*reply = RequestTaskReply {false, false, true, c.files, reduceN}
			c.reduceTasks[reduceN] = Task{dispatched, time.Now()}
			return nil
			break
		}
	}
	//TODO handle nill RequestTaskReply in worker.go
	*reply = RequestTaskReply{false, false, false, nil, 0}
	return nil
}

func (c *Coordinator) ProgressReduce(reduceN *int, reply *ProgressReduceReply) error {
	doneMapTasks := make([]string, 0)
	for file, task := range c.mapTasks {
		if task.state == finished {
			doneMapTasks = append(doneMapTasks, file)
		}
	}
	c.reduceTasks[*reduceN] = Task{dispatched, time.Now()}
	*reply = doneMapTasks
	return nil
}

//notify coordinator that work is done
func (c *Coordinator) Finish(args *FinishArgs, reply *NullArgs) error {
	if args.IsMapTask == true {
		// TODO: lock
		c.mapTasks[args.Files[0]] = Task{finished, time.Now()}
	} else {
		// TODO: lock
		c.reduceTasks[args.ReduceN] = Task{finished, time.Now()}
	}

	return nil
}

func (c *Coordinator) StatusReport() {
	fmt.Println(c.workerStates)
	fmt.Println(c.mapTasks)
	fmt.Println(c.reduceTasks)
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.
	//if all nReduce reduce jobs finish, return ture
	if c.isDone {
		return true
	}
	for _, task := range c.reduceTasks {
		if task.state != finished {
			return false
		}
	}
	c.isDone = true
	return true
}

func (c *Coordinator) MapDone() bool {
	if c.isMapDone {
		return true
	}
	for _, task := range c.mapTasks {
		if task.state != finished {
			return false
		}
	}
	c.isMapDone = true
	return true
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce,
		files,
		make(map[string]worker),
		make(map[string]Task),
		make(map[int]Task),
		false,
		false}

	// Your code here.
	/*
	c.nReduce = nReduce
	c.files = files
	c.workerStates = make(map[string]worker)
	c.mapTasks = make(map[string]Task)
	c.reduceTasks = make(map[int]Task)
	c.isMapDone = false
	c.isDone = false
	*/
	//initialize mapTasks
	for _, inputfile := range files{
		c.mapTasks[inputfile] = Task{dispatchable, time.Now()}
	}
	//initialize reduceTasks
	//TODO reduce tasks shouldn't be dispatchable at first
	for i := 0; i < nReduce; i++ {
		//TODO need to change, dispatchable after map
		c.reduceTasks[i] = Task{dispatchable, time.Now()}
	}

	c.server()
	return &c
}
