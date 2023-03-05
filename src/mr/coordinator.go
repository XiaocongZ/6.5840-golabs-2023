package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "fmt"
import "time"
import "sync"

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
	mapTasks sync.Map
	reduceTasks sync.Map
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
	pleaseExit := RequestTaskReply{true, false, false, nil, 0}
	pleaseWait := RequestTaskReply {false, false, false, nil, 0}
	if c.Done() {
		//tell worker to exit
		*reply = pleaseExit
		return nil
	}
	/*
	for file, task := range c.mapTasks {
		if task.state == dispatchable {
			//TODO dispatch lock

			*reply = RequestTaskReply {false, true, false, []string{file}, c.nReduce}
			c.mapTasks[file] = Task{dispatched, time.Now()}
			return nil
			break
		}
	}
	*/
	c.mapTasks.Range(
		func(k , v interface{}) bool {
			file, _ := k.(string)
			task, _ := v.(Task)
			if task.state == dispatchable {
				*reply = RequestTaskReply {false, true, false, []string{file}, c.nReduce}
				c.mapTasks.Store( file , Task{dispatched, time.Now()} )
				return false
			}
			return true
		})
	if reply.IsMapTask {
		return nil
	}
	//TODO parallel map & reduce
	if ! c.MapDone() {
		//tell worker to wait
		*reply = pleaseWait
		return nil
	}
	/*
	for reduceN, task := range c.reduceTasks {
		if task.state == dispatchable {
			//TODO dispatch lock
			*reply = RequestTaskReply {false, false, true, c.files, reduceN}
			c.reduceTasks[reduceN] = Task{dispatched, time.Now()}
			return nil
			break
		}
	}
	*/
	c.reduceTasks.Range(
		func(k, v interface{}) bool {
			reduceN, _ := k.(int)
			task, _ := v.(Task)
			if task.state == dispatchable {
				*reply = RequestTaskReply {false, false, true, c.files, reduceN}
				c.reduceTasks.Store( reduceN , Task{dispatched, time.Now()} )
				return false
			}
			return true
		})
	if reply.IsReduceTask {
		return nil
	} else {
		//likely all dispatched, wait
		*reply = pleaseWait
		return nil
	}

}
/*
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
*/
//notify coordinator that work is done
func (c *Coordinator) Finish(args *FinishArgs, reply *NullArgs) error {
	if args.IsMapTask == true {
		// TODO: lock
		c.mapTasks.Store( args.Files[0], Task{finished, time.Now()} )
	} else {
		// TODO: lock
		c.reduceTasks.Store( args.ReduceN, Task{finished, time.Now()} )
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
	//if all nReduce reduce jobs finish, return ture
	if c.isDone {
		return true
	}
	isDone := true

	c.reduceTasks.Range(
		func(k, v interface{}) bool {
			//reduceN, _ := k.(int)
			task, _ := v.(Task)
			if task.state != finished {
				isDone = false
				return false
			}
			return true
		})
	if isDone {
		c.isDone = true
		return true
	} else {
		return false
	}

}

func (c *Coordinator) MapDone() bool {
	if c.isMapDone {
		return true
	}
	isMapDone := true // tentative true value

	c.mapTasks.Range(
		func(k, v interface{}) bool {
			//file, _ := k.(string)
			task, _ := v.(Task)
			if task.state != finished {
				isMapDone = false
				return false
			}
			return true
		})

	if isMapDone {
		c.isMapDone = true
		return true
	} else {
		return false
	}
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
		sync.Map{},
		sync.Map{},
		false,
		false}

	// Your code here.
	//initialize mapTasks
	for _, inputfile := range files{
		c.mapTasks.Store( inputfile , Task{dispatchable, time.Now()} )
	}

	//initialize reduceTasks
	//TODO reduce tasks shouldn't be dispatchable at first
	for i := 0; i < nReduce; i++ {
		//TODO need to change, dispatchable after map
		c.reduceTasks.Store( i , Task{dispatchable, time.Now()} )
	}

	c.server()
	return &c
}
