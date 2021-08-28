package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

var ReduceFiles []string

type Master struct {
	// Your definitions here.

	mu sync.Mutex

	MapTasks    []string
	ReduceTasks []int //储存reduce task每一部分都是由
	//哪个workder生成的。方便传递目录

	MapStatus    []bool //每个task完成的状态，一个task可能包含多个file
	ReduceStatus []bool

	MapFinished    int //map task未完成的数目
	ReduceFinished int //reduce task未完成的数目

	Nreduce int

	MapHead    *TaskNode
	ReduceHead *TaskNode

	MapPointer    *TaskNode //对task进行分配时使用的指针
	ReducePointer *TaskNode

	workers int //worker的数量

	send chan *TaskNode //一个分配任务的channel

	receive chan ReplyNode //一个接受结果的channel

	mapArray    []*TaskNode
	reduceArray []*TaskNode
}

type TaskNode struct {
	TaskId int //the task that if handled
	next   *TaskNode
	prev   *TaskNode

	Cat int //which kind of task
	//Finished bool //if finish a given task
	WorkerId int //the id of the worker
	FileName string

	status int // 0:unassigned, 1:assigned but unfinished,2:finished
}

//receive through channel from workers
type ReplyNode struct {
	TaskId   int
	FileName string
	Cat      int
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) WorkerHandler(args *StateArgs, reply *StateReply) error {

	m.mu.Lock()

	reply.send = m.send
	reply.receive = m.receive
	reply.WorkerId = m.workers + 1
	reply.Nreduce = m.Nreduce

	m.workers++

	//fmt.Println("assign a worker ", strconv.Itoa(m.workers))

	m.mu.Unlock()

	return nil
}

func (m *Master) TaskHandler(args *StateArgs, reply *StateReply) error {

	//m.mu.Lock()

	if args.Finished {

		m.receive <- ReplyNode{args.TaskId, args.FileName, args.Cat}
		//fmt.Println("receive channel", len(m.receive))
	}

	task, ok := <-m.send

	if !ok {
		reply.Cat = -1
		return nil
	}

	reply.Cat = task.Cat
	reply.FileName = task.FileName
	reply.TaskId = task.TaskId

	//fmt.Println("ready to send to worker", "cat", reply.Cat, "taskId", reply.TaskId)

	if task.Cat == 1 {

		reply.F = m.ReduceFinished
		reply.FileNames = ReduceFiles
	}

	//m.mu.Unlock()

	return nil
}

func (m *Master) assignTask() {

	mpp := m.MapPointer    //map task pointer
	rdp := m.ReducePointer //reduce task pointer

	for m.workers == 0 {
	}

	for ; mpp.next != mpp; mpp = mpp.next {

		if mpp == m.MapHead {

		} else if mpp.status == 0 {

			mpp.status = 1
			//fmt.Println("assign a mapTask")
			m.send <- mpp
		} else if mpp.status == 1 {

			//fmt.Println("wait worker")
			time.Sleep(time.Duration(50) * time.Millisecond)
			if mpp.status == 2 {
				DeleteNode(mpp)
			} else {

				mpp.status = 1
				//fmt.Println("assign a mapTask")
				m.send <- mpp

			}

		} else {
			DeleteNode(mpp)
		}

	}

	if m.MapFinished != 0 {
	}

	//fmt.Println("send all mapTask")

	for ; rdp.next != rdp; rdp = rdp.next {

		if rdp == m.ReduceHead {

		} else if rdp.status == 0 {

			rdp.status = 1
			//fmt.Println("assign a reduceTask ", rdp.TaskId)
			m.send <- rdp
			//fmt.Println("send channel", len(m.send))
		} else if rdp.status == 1 {

			//fmt.Println("wait worker")
			time.Sleep(time.Duration(50) * time.Millisecond)
			if rdp.status == 2 {
				DeleteNode(rdp)
			} else {

				rdp.status = 1
				//fmt.Println("assign a reduceTask", rdp.TaskId)
				m.send <- rdp
			}

		} else {
			DeleteNode(rdp)
		}

	}

	close(m.send)

}

func (m *Master) assign(mpp *TaskNode) {

	for ; mpp.next != mpp; mpp = mpp.next {

		if mpp == m.MapHead {

		} else if mpp.status == 0 {

			m.send <- mpp
		} else if mpp.status == 1 {

			time.Sleep(time.Duration(50) * time.Millisecond)
			if mpp.status == 2 {
				DeleteNode(mpp)
			} else {

				m.send <- mpp

			}

		} else {
			DeleteNode(mpp)
		}

	}

}

func (m *Master) receiveTask() {

	for t := range m.receive {

		file := t.FileName
		cat := t.Cat

		var task *TaskNode

		if cat == 0 {

			task = m.mapArray[t.TaskId]

		} else {

			task = m.reduceArray[t.TaskId-1]
		}

		//fmt.Println("process ", cat, task.TaskId)

		if task.Cat == 0 && task.status != 2 {

			m.MapStatus[task.TaskId] = true
			ReduceFiles[task.TaskId] = file
			task.status = 2

			m.MapFinished--

		} else if task.Cat == 1 && task.status != 2 {

			m.ReduceStatus[task.TaskId-1] = true
			task.status = 2
			os.Rename(file, "mr-out-"+strconv.Itoa(task.TaskId-1))

			m.ReduceFinished--
		}

		if m.ReduceFinished == 0 {
			break
		}

	}

}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	m.mu.Lock()
	if m.MapFinished == 0 && m.ReduceFinished == 0 {

		ret = true

		//os.RemoveAll("../../lab_interFiles")

	}
	m.mu.Unlock()

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//

func DeleteNode(node *TaskNode) {

	temp := node.prev
	temp.next = node.next
	node.next.prev = temp

}
func AddNode(head *TaskNode, node *TaskNode) {

	temp := head.prev

	temp.next = node
	node.prev = temp
	node.next = head
	head.prev = node

}

func InitHead(head *TaskNode) {

	head.next = head
	head.prev = head
	head.TaskId = -1

}

func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.

	m.MapTasks = files
	m.MapFinished = len(files)
	m.Nreduce = nReduce
	m.ReduceFinished = nReduce
	m.MapStatus = make([]bool, len(files))
	m.ReduceStatus = make([]bool, nReduce)
	m.MapHead = new(TaskNode)
	m.ReduceHead = new(TaskNode)

	m.ReduceTasks = make([]int, len(files))

	InitHead(m.MapHead)
	InitHead(m.ReduceHead)

	m.mapArray = make([]*TaskNode, len(files))
	m.reduceArray = make([]*TaskNode, nReduce)

	for i := 0; i < len(files); i++ {

		node := &TaskNode{TaskId: i, Cat: 0, FileName: files[i]}
		AddNode(m.MapHead, node)
		m.mapArray[i] = node

	}

	for i := 0; i < nReduce; i++ {

		node := &TaskNode{TaskId: i + 1, Cat: 1}
		AddNode(m.ReduceHead, node)
		m.reduceArray[i] = node
	}

	m.MapPointer = m.MapHead.next
	m.ReducePointer = m.ReduceHead.next

	os.Mkdir("lab_interFiles", 0755)

	m.send = make(chan *TaskNode, 5)
	m.receive = make(chan ReplyNode, 5)

	ReduceFiles = make([]string, len(files))

	m.server()

	go func() {

		m.assignTask()

	}()
	go func() {

		m.receiveTask()

	}()

	return &m
}
