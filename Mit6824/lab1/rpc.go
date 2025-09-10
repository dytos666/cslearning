package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"log"
	"os"
	"strconv"
	"time"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}
type ExampleReply struct {
	Y int
}
type MapTask struct {
	filename  string
	status    int       // 0:未分配 1:分配中 2:完成
	startTime time.Time // 记录分配给Worker的时间
	taskId    int
}
type ReduceTask struct {
	status    int       // 0:未分配 1:分配中 2:完成
	startTime time.Time // 记录分配给Worker的时间
	reduceId  int
}

// Add your RPC definitions here.

// 1. wanna work
// 2. success
// 3. fail
var Debug = false

// 日志颜色
const (
	ColorReset  = "\033[0m"
	ColorRed    = "\033[31m"
	ColorGreen  = "\033[32m"
	ColorYellow = "\033[33m"
	ColorBlue   = "\033[34m"
)

// 带颜色的调试打印
func DPrintf(prefix string, color string, format string, a ...interface{}) {
	if Debug {
		log.Printf(color+"%-5s "+ColorReset+format, append([]interface{}{prefix}, a...)...)
	}
}

type Args struct {
	ExecuteType int
	Flag        int
	MapId       int
	ReduceId    int
}

func (args *Args) Failed() {
	args.ExecuteType = 3
}
func (args *Args) Wanned() {
	args.ExecuteType = 1
}
func (args *Args) Succeed() {
	args.ExecuteType = 2
}

type Reply struct {
	Filename string
	MapId    int
	ReduceId int
	NReduce  int
	NTask    int
	Flag     int
	Done     bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
