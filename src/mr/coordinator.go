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
	mutex sync.Mutex
	state int
	timeStamp time.Time
}
//obtain unlocked dispatchable task and expired dispatched task
func (t *Task) tryObtain() bool {
	if t.state == finished || t.state == nullTask {
		return false
	}
	// state is dispatchable or dispatched
	if t.mutex.TryLock() {
		defer t.mutex.Unlock()
		if t.state == dispatchable {
			t.state = dispatched
			t.timeStamp = time.Now()
			return true
		} else if t.state == dispatched && time.Now().Sub(t.timeStamp).Seconds() > 5 {
			t.timeStamp = time.Now()
			return true
		}
	}
	//missed lock
	return false
}
func (t *Task) finish() bool {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if t.state == finished {
		fmt.Println("A Task finished twice")
		return true
	} else if t.state == dispatched {
		t.state = finished
		t.timeStamp = time.Now()
		return true
	} else {
		fmt.Printf("Task state %d", t.state)
		panic("Task cannot finish: wrong state transition")
	}
}

//enum for Task.state
const (
	nullTask int = iota //not initialized
	dispatchable
	dispatched
	finished
)
//map is not thread safe, item in map cannot be pointer receive due to rehash; use slice instead.
//each Task has a mutex
type Coordinator struct {
	// Your definitions here.
	nReduce int
	nMap int
	files []string
	workerStates map[string]worker
	mapTasks []Task
	reduceTasks []Task
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

	for i, _ := range c.mapTasks {
		pTask := &c.mapTasks[i]
		if pTask.tryObtain() {
			*reply = RequestTaskReply {false, true, false, []string{c.files[i]}, c.nReduce}
			return nil
		}
	}

	//TODO parallel map & reduce
	if ! c.MapDone() { //tell worker to wait
		*reply = pleaseWait
		return nil
	}

	for reduceN, _ := range c.reduceTasks {
		pTask := &c.reduceTasks[reduceN]
		if pTask.tryObtain() {
			*reply = RequestTaskReply {false, false, true, c.files, reduceN}
			return nil
		}
	}
	//likely all dispatched, wait
	*reply = pleaseWait
	return nil
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
		//c.mapTasks[args.Files[0]].finish()
		for i, _ := range c.mapTasks {
			if c.files[i] == args.Files[0] {
				c.mapTasks[i].finish()
				break
			}
		}
	} else {
		c.reduceTasks[args.ReduceN].finish()
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
		len(files),
		files,
		make(map[string]worker),
		make([]Task, len(files)),
		make([]Task, nReduce),
		false,
		false}

	// Your code here.
	//initialize mapTasks
	for i, _ := range files{
		c.mapTasks[i] = Task{sync.Mutex{}, dispatchable, time.Now()}
	}

	//initialize reduceTasks
	//TODO reduce tasks shouldn't be dispatchable at first
	for i := 0; i < nReduce; i++ {
		//TODO need to change, dispatchable after map
		c.reduceTasks[i] = Task{sync.Mutex{}, dispatchable, time.Now()}
	}
	//start multi-threading
	c.server()
	return &c
}
