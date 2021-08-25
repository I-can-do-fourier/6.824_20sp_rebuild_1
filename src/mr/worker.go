package mr

import (
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

var args StateArgs = StateArgs{0, 0, false, true, 0, ""}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	for {

		//args := StateArgs{0, 0, false, true}
		reply := StateReply{}
		//fmt.Println("calling the server")
		call("Master.RequestHandler", &args, &reply)
		//fmt.Println("receive task from server")

		args.Cat = reply.Cat
		args.TaskId = reply.TaskId
		args.WorkerId = reply.WorkerId
		args.Finished = false

		if reply.Cat == 1 {

			args.Idle = false

			fmt.Printf("cat: mapTask,file: %v,taskId: %v\n",
				reply.FileName, reply.TaskId)
			mapTask(mapf, reply.FileName, reply.Nreduce, reply.TaskId)

		} else if reply.Cat == 2 {

			args.Idle = false

			fmt.Printf("cat: reduceTask,taskId: %v\n", reply.TaskId)
			reduceTask(reducef, reply)

		} else {

			return

		}

	}

}

func mapTask(mapf func(string, string) []KeyValue, filename string, nreduce int,
	taskId int) {

	intermediate := []KeyValue{}

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))

	intermediate = append(intermediate, kva...)

	//sort.Sort(main.ByKey(intermediate))

	//record the position of keyValue for each reducetask
	//将不同hash的key归类，一共nreduce类
	positions := make([][]int, nreduce)

	for index := range positions {

		positions[index] = make([]int, 0)

	}

	for i := 0; i < len(intermediate); i++ {

		hash := ihash(intermediate[i].Key) % nreduce
		positions[hash] = append(positions[hash], i)

	}

	for i := 0; i < nreduce; i++ {

		path := "lab_interFiles/" + "mr-" + strconv.Itoa(taskId) + "-" + strconv.Itoa(i) + "-" + strconv.Itoa(args.WorkerId)
		//dic := "../lab_interFiles/"
		//randomString := "mr-" + strconv.Itoa(taskId) + "-" + strconv.Itoa(i) + "-"
		f, err := os.Create(path)
		//f, err := ioutil.TempFile(dic, randomString)
		if err != nil {
			panic(err)
		}

		//defer os.Remove(f.Name())

		for _, p := range positions[i] {

			fmt.Fprintf(f, "%v %v\n", intermediate[p].Key, intermediate[p].Value)

		}

	}

	args.Finished = true
	args.Idle = true

}

func reduceTask(reducef func(string, []string) string, reply StateReply) {

	intermediate := readInterFiles(reply.TaskId, reply.MapCount, reply.FileNames)

	sort.Sort(ByKey(intermediate))
	oname := "out-" + strconv.Itoa(reply.TaskId) + "-" + strconv.Itoa(reply.WorkerId)
	//nname := "mr-out-" + strconv.Itoa(reply.TaskId)
	ofile, _ := os.Create(oname)
	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	//os.Rename(oname, nname)
	ofile.Close()

	args.Finished = true
	args.Idle = true
	args.FileName = oname

}

//for a certain reduce task,load all the (key,value) pair
//from intermediate files
//n is the number of files to load which is equal to the
//number of the map tasks
func readInterFiles(taskId int, n int, FileNames []int) []KeyValue {

	pairs := []KeyValue{}

	for index, item := range FileNames {

		path := "lab_interFiles/" + "mr-" + strconv.Itoa(index) + "-" + strconv.Itoa(taskId) + "-" + strconv.Itoa(item)
		file, err := os.Open(path)

		if err != nil {
			log.Fatalf("cannot open %v", file)
		}

		defer file.Close()

		var er error

		for er != io.EOF && er == nil {

			var k string
			var v string
			_, er = fmt.Fscanf(file, "%s %s\n", &k, &v)

			pairs = append(pairs, KeyValue{Key: k, Value: v})
		}

		pairs = pairs[0 : len(pairs)-1] //删掉最后一个空的

	}

	return pairs

}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
