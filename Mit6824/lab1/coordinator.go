package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	mapTasks    []MapTask
	reduceTasks []ReduceTask
	lock_       sync.Mutex
	taskNum_    int
	reduceNum_  int
	nReduce_    int
	nTask_      int
}

func (c *Coordinator) Handout_map(reply *Reply) {
	c.lock_.Lock()
	for i, task := range c.mapTasks {
		if task.status == 0 {
			DPrintf("[C]", ColorBlue, "handout filename:%v  taskId: %d\n", task.filename, task.taskId)
			reply.Filename = task.filename
			reply.Flag = 1
			reply.NReduce = c.nReduce_
			reply.NTask = c.nTask_
			reply.MapId = task.taskId
			c.mapTasks[i].startTime = time.Now()
			c.mapTasks[i].status = 1

			break
		}
	}
	c.lock_.Unlock()
}
func (c *Coordinator) Handoutap_reduce(reply *Reply) {

	c.lock_.Lock()
	for i, task := range c.reduceTasks {
		if task.status == 0 {
			DPrintf("[C]", ColorBlue, "  hanout              reduceId: %d\n", task.reduceId)
			reply.Flag = 2
			reply.NReduce = c.nReduce_
			reply.NTask = c.nTask_
			reply.ReduceId = task.reduceId

			c.reduceTasks[i].startTime = time.Now()
			c.reduceTasks[i].status = 1

			break
		}
	}
	c.lock_.Unlock()
}

func (c *Coordinator) HandleSuccess(args *Args) {

	c.lock_.Lock()
	if args.Flag == 1 {
		for i, task := range c.mapTasks {
			if task.status == 1 && task.taskId == args.MapId {
				DPrintf("[C]", ColorBlue, "Success  taskId: %d\n", task.taskId)
				c.mapTasks[i].status = 2
				c.taskNum_++
				break
			}
		}
	} else if args.Flag == 2 {
		for i, task := range c.reduceTasks {
			if task.status == 1 && task.reduceId == args.ReduceId {
				DPrintf("[C]", ColorBlue, "Success  ReduceId: %d\n", task.reduceId)
				c.reduceTasks[i].status = 2
				c.reduceNum_++
				break
			}
		}

	}
	c.lock_.Unlock()

}

func (c *Coordinator) HandleError(args *Args) {

	c.lock_.Lock()
	if args.Flag == 1 {
		for i, task := range c.mapTasks {
			if task.status == 1 && task.taskId == args.MapId {
				DPrintf("[C]", ColorBlue, "Error  taskId: %d\n", task.taskId)
				c.mapTasks[i].status = 0
				c.mapTasks[i].startTime = time.Time{}
				break
			}
		}
	} else if args.Flag == 2 {
		for i, task := range c.reduceTasks {
			if task.status == 1 && task.reduceId == args.ReduceId {
				DPrintf("[C]", ColorBlue, "Error  ReduceId: %d\n", task.reduceId)
				c.reduceTasks[i].status = 0
				c.reduceTasks[i].startTime = time.Time{}
				break
			}
		}

	}
	c.lock_.Unlock()

}
func (c *Coordinator) Faultsearch() {
	for {
		time.Sleep(time.Second) // 每秒检查一次
		c.lock_.Lock()
		for i, task := range c.mapTasks {
			if task.status == 1 && time.Since(task.startTime) > 10*time.Second {
				// 超时，重置状态
				DPrintf("[C]", ColorBlue, "reastart  taskId: %d\n", task.taskId)
				c.mapTasks[i].status = 0
				c.mapTasks[i].startTime = time.Time{}
				DPrintf("[C]", ColorBlue, "map task %v timeout, reassigning\n", task.filename)
			}
		}
		for i, task := range c.reduceTasks {
			if task.status == 1 && time.Since(task.startTime) > 10*time.Second {
				// 超时，重置状态
				DPrintf("[C]", ColorBlue, "reastart  ReduceId: %d\n", task.reduceId)
				c.reduceTasks[i].status = 0
				c.reduceTasks[i].startTime = time.Time{}
				DPrintf("[C]", ColorBlue, "reduce task %v timeout, reassigning\n", task.reduceId)
			}
		}
		c.lock_.Unlock()
	}
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RpcHandler(args *Args, reply *Reply) error {
	//fmt.Printf("handler\n")
	if c.Done() {
		reply.Flag = 3
		reply.Done = true
		return nil
	}

	if args.ExecuteType == 1 {

		if c.nTask_ == c.taskNum_ {
			c.Handoutap_reduce(reply)
		} else {
			c.Handout_map(reply)
		}

	} else if args.ExecuteType == 2 {
		c.HandleSuccess(args)
	} else {
		c.HandleError(args)
		reply.Flag = 4

	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.lock_.Lock()
	defer c.lock_.Unlock()
	if c.taskNum_ == c.nTask_ && c.reduceNum_ == c.nReduce_ {
		DPrintf("[SUCC]", ColorGreen, "everything down\n")
		return true

	}
	return false
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapTasks:    make([]MapTask, 0),
		reduceTasks: make([]ReduceTask, 0),
		lock_:       sync.Mutex{},
		taskNum_:    0,
		reduceNum_:  0,
		nTask_:      len(files),
		nReduce_:    nReduce,
	}

	for i, value := range files {
		task := MapTask{
			filename:  value,
			status:    0,
			startTime: time.Time{},
			taskId:    i,
		}
		c.mapTasks = append(c.mapTasks, task)

	}
	for i := 0; i < nReduce; i++ {
		task := ReduceTask{
			status:    0,
			startTime: time.Time{},
			reduceId:  i,
		}
		c.reduceTasks = append(c.reduceTasks, task)
	}

	go c.Faultsearch()
	c.server()

	DPrintf("[C]", ColorBlue, "mapnum:%d  reducenum: %d\n", c.nTask_, c.nReduce_)
	return &c
}
