package distributed

import (
	"encoding/json"
	"fmt"
	"gomr.com/gomr/mr"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"
)

type State string

const (
	Unassigned State = "unassigned"
	Assigned   State = "assigned"
	Completed  State = "completed"
)

const reduceDirPath = "/tmp/gomr/reduce"

type task struct {
	state     State
	startTime time.Time
	mx        sync.Mutex
	filename  string
}

func (t *task) timeout() bool {
	if time.Since(t.startTime) >= 30*time.Second {
		return true
	}
	return false
}

func (t *task) assignTask() {
	t.state = Assigned
	t.startTime = time.Now()
}

/**
Represents structure for Controller Node.
*/
type Controller struct {
	uuid                 string
	taskTimeout          time.Duration
	numReduce            int //number of reduce tasks
	numMap               int //number of map tasks
	mapTasks             map[int]*task
	reduceTasks          map[int]*task
	mapTasksCompleted    bool
	reduceTasksCompleted bool
}

/**
Helper functions
*/

/**
Combines all the reduce mr-numMap-numReduce files into mr-reduce-numReduce.

Basically combines all the inputs for a particular reduce partition from all the map
operation into a single reduce file.
*/
func (c *Controller) sortIntermediate() {
	log.Printf("Starts Combining Reduce partition in all map operations")
	log.Printf("The reduce files output %s", reduceDirPath)
	//remove everything from temp directory
	os.RemoveAll(reduceDirPath)
	err := os.Mkdir(reduceDirPath, os.ModePerm)

	if err != nil {
		log.Printf("Failed to create temp directory: %v for processing, err: %v", reduceDirPath, err)
	}

	for i := 0; i < c.numReduce; i++ {

		if err != nil {
			log.Printf("Failed to create temp directory: %v for processing, err: %v", reduceDirPath, err)
		}

		reduceFileName := fmt.Sprintf("mr-reduce-%d", i)
		reduceFile, err := os.OpenFile(
			filepath.Join(reduceDirPath, reduceFileName), os.O_RDWR|os.O_CREATE|os.O_EXCL, os.ModePerm,
		)
		if err != nil {
			log.Fatalf(
				"Failed to create output file for dir: %v and filename %v, err: %v", reduceDirPath,
				reduceFileName, err,
			)
		}

		encoder := json.NewEncoder(reduceFile)
		keyValueArr := []mr.KeyValue{}
		for j := 0; j < c.numMap; j++ {
			mapPartitionFileName := fmt.Sprintf("mr-%d-%d", j, i)
			mapPartitionFile, err := os.Open(filepath.Join(mapOutputDirName, mapPartitionFileName))
			if err != nil {
				log.Printf("Warn: Unable to open the mapPartition File %v, err: %v", mapPartitionFileName, err)
			}
			decoder := json.NewDecoder(mapPartitionFile)

			for {
				var kv mr.KeyValue
				if err := decoder.Decode(&kv); err != nil {
					break
				}
				keyValueArr = append(keyValueArr, kv)
			}
			mapPartitionFile.Close()
		}
		sort.Sort(mr.SortKey(keyValueArr))

		for _, kv := range keyValueArr {
			encoder.Encode(&kv)
		}
		c.reduceTasks[i].filename = reduceFileName
		reduceFile.Close()
	}
}

/*
Check for all the Map tasks completion.
*/
func (c *Controller) isMapTaskCompleted() bool {
	if c.mapTasksCompleted == true {
		return true
	}
	for _, t := range c.mapTasks {
		t.mx.Lock()
		if t.state != Completed {
			t.mx.Unlock()
			return false
		}
		t.mx.Unlock()
	}
	c.mapTasksCompleted = true
	c.sortIntermediate()
	return true
}

func (c *Controller) assignMapTask() int {
	taskId := -1
	for i, t := range c.mapTasks {
		t.mx.Lock()
		if t.state == Completed || (t.state == Assigned && !t.timeout()) {
			t.mx.Unlock()
			continue
		}
		if t.timeout() {
			log.Printf("Assigning timed out task %d \n", i)
		}
		t.assignTask()
		taskId = i
		log.Printf("Assigning Task %d to the worker", i)
		t.mx.Unlock()
		break
	}
	if taskId == -1 {
		log.Printf("Not available free Map Task Found")
	}
	return taskId
}

/*
Check for all the Map tasks completion.
*/
func (c *Controller) isReduceTaskCompleted() bool {
	if c.reduceTasksCompleted == true {
		return true
	}
	for _, t := range c.reduceTasks {
		t.mx.Lock()
		if t.state != Completed {
			t.mx.Unlock()
			return false
		}
		t.mx.Unlock()
	}
	c.reduceTasksCompleted = true
	return true
}

func (c *Controller) assignReduceTask() int {
	taskId := -1
	for i, t := range c.reduceTasks {
		t.mx.Lock()
		if t.state == Completed || (t.state == Assigned && !t.timeout()) {
			t.mx.Unlock()
			continue
		}
		if t.timeout() {
			log.Printf("Assigning timed out task %d \n", i)
		}
		t.assignTask()
		taskId = i
		log.Printf("Assigning Task %d to the worker", i)
		t.mx.Unlock()
		break
	}
	if taskId == -1 {
		log.Printf("Not available free Reduce Task Found")
	}
	return taskId
}

/**
APIS
*/

/**
Map
*/

func (c *Controller) CheckForMapTasksCompletion(
	request *CheckForMapTasksCompletionRequest, response *CheckForMapTasksCompletionResponse,
) error {
	log.Println("CheckForMapTasksCompletion Called")
	response.AllCompleted = c.isMapTaskCompleted()
	return nil
}

func (c *Controller) GetMapTask(
	request *GetMapTaskRequest,
	response *GetMapTaskResponse,
) error {
	log.Println("GetMapTask Called")
	if c.mapTasksCompleted {
		response.TaskId = -1
		return nil
	}
	taskId := c.assignMapTask()
	response.TaskId = taskId
	response.NumReduce = c.numReduce
	if taskId != -1 {
		response.Filename = c.mapTasks[taskId].filename
	}
	return nil
}

func (c *Controller) UpdateMapTask(
	request *UpdateMapTaskRequest,
	response *UpdateMapTaskResponse,
) error {

	log.Printf("Handling request for the completion of MapTask: %d", request.TaskId)
	task := c.mapTasks[request.TaskId]
	task.mx.Lock()
	defer task.mx.Unlock()

	if task.state == Completed {
		log.Printf("The task: %d already completed by other worker", request.TaskId)
		return nil
	}
	task.state = Completed
	log.Printf("Handled UpdateMap Task as completed for taskId: %d", request.TaskId)
	return nil
}

/*
Reduce
*/
func (c *Controller) CheckForReduceTasksCompletion(
	request *CheckForReduceTasksCompletionRequest, response *CheckForReducdTasksCompletionResponse,
) error {
	log.Println("CheckForReduceTasksCompletion Called")
	response.AllCompleted = c.isReduceTaskCompleted()
	return nil
}

func (c *Controller) GetReduceTask(
	request *GetReduceTaskRequest,
	response *GetReduceTaskResponse,
) error {
	log.Println("GetReduceTask Called")
	if c.reduceTasksCompleted {
		response.TaskId = -1
		return nil
	}
	taskId := c.assignReduceTask()
	response.TaskId = taskId
	if taskId != -1 {
		response.Filename = c.reduceTasks[taskId].filename
	}
	return nil
}

func (c *Controller) UpdateReduceTask(
	request *UpdateReduceTaskRequest,
	response *UpdateReduceTaskResponse,
) error {
	log.Printf("Handling request for the completion of ReduceTask: %d", request.TaskId)
	task := c.reduceTasks[request.TaskId]
	task.mx.Lock()
	defer task.mx.Unlock()

	if task.state == Completed {
		log.Printf("The task: %d already completed by other worker", request.TaskId)
		return nil
	}
	task.state = Completed
	log.Printf("Handled UpdateReduceTask as completed for taskId: %d", request.TaskId)
	return nil

}

/**
Starts the Controller given the list of files and the number of reduce tasks to use.
*/

func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

func (c *Controller) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)

}

func (c *Controller) Done() bool {
	if c.mapTasksCompleted && c.reduceTasksCompleted {
		return true
	}
	return false
}

func MakerController(files []string, nReduce int) *Controller {
	c := Controller{}
	uuid, err := exec.Command("uuidgen").Output()
	if err != nil {
		log.Fatal("Unable to generate UUID: %s", err)
	}

	c.uuid = string(uuid)
	c.taskTimeout = 10 * time.Second
	c.numMap = len(files)
	c.numReduce = nReduce
	c.mapTasks = make(map[int]*task)
	c.reduceTasks = make(map[int]*task)
	c.mapTasksCompleted = false
	c.reduceTasksCompleted = false

	for i := 0; i < c.numMap; i++ {
		c.mapTasks[i] = &task{
			filename:  files[i],
			state:     Unassigned,
			startTime: time.Now(),
		}
	}

	for i := 0; i < c.numReduce; i++ {
		c.reduceTasks[i] = &task{
			state:     Unassigned,
			startTime: time.Now(),
			filename:  "",
		}
	}
	c.server()
	return &c
}
