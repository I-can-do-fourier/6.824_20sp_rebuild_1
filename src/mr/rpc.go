package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
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

// Add your RPC definitions here.

type StateArgs struct {
	Cat      int  //which kind of task
	TaskId   int  //the task that if handled
	Finished bool //if finish a given task

	Idle     bool //if idle
	WorkerId int  //the id of the worker

	FileName string

	//ch chan int
}

type StateReply struct {
	Cat       int    //which kind of task
	TaskId    int    //task id
	FileName  string //which file should be  handled
	Nreduce   int
	MapCount  int // the number of map tasks
	WorkerId  int
	FileNames []string // the file names for reduce task

	send    chan *TaskNode
	receive chan ReplyNode

	F int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
