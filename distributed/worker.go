package distributed

import (
	"encoding/json"
	"fmt"
	"gomr.com/gomr/mr"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"time"
)

const mapOutputDirName = "/tmp/gomr/map"
const outputDirName = "/tmp/gomr/output"

func checkForMapTasksCompletion() bool {
	log.Printf("Calling Controller.CheckForMapTasksCompletion")
	request := CheckForMapTasksCompletionRequest{}
	response := CheckForMapTasksCompletionResponse{}
	call("Controller.CheckForMapTasksCompletion", &request, &response)
	log.Printf("Got ther response form Controller.CheckForMapTasksCompletion: %v\n", response)
	return response.AllCompleted
}

func getMapTask() (int, string, int) {
	log.Printf("Calling Controller.GetMapTask")
	request := GetMapTaskRequest{}
	response := GetMapTaskResponse{}
	call("Controller.GetMapTask", &request, &response)
	log.Printf("Got the response form Controller.GetMapTask: %v\n", response)
	return response.TaskId, response.Filename, response.NumReduce
}

func updateMapTaskWithCompletion(taskId int) error {
	log.Printf("Calling Controller.UpdateMapTask")
	request := UpdateMapTaskRequest{TaskId: taskId}
	response := UpdateMapTaskResponse{}
	call("Controller.UpdateMapTask", &request, &response)
	log.Printf("Got the response form Controller.UpdateMapTask: %v\n", response)
	return nil
}

func checkForReduceTasksCompletion() bool {
	log.Printf("Calling Controller.CheckForReduceTasksCompletion")
	request := CheckForReduceTasksCompletionRequest{}
	response := CheckForReducdTasksCompletionResponse{}
	call("Controller.CheckForReduceTasksCompletion", &request, &response)
	log.Printf("Got ther response form Controller.CheckForReduceTasksCompletion: %v\n", response)
	return response.AllCompleted
}

func getReduceTask() (int, string) {
	log.Printf("Calling Controller.GetReduceTask")
	request := GetReduceTaskRequest{}
	response := GetReduceTaskResponse{}
	call("Controller.GetReduceTask", &request, &response)
	log.Printf("Got the response form Controller.GetReduceTask: %v\n", response)
	return response.TaskId, response.Filename
}

func updateReduceTaskWithCompletion(taskId int) error {
	log.Printf("Calling Controller.UpdateReduceTask")
	request := UpdateReduceTaskRequest{TaskId: taskId}
	response := UpdateReduceTaskResponse{}
	call("Controller.UpdateReduceTask", &request, &response)
	log.Printf("Got the response form Controller.UpdateReduceTask: %v\n", response)
	return nil
}

func Mapper(
	mapf func(string, string) []mr.KeyValue,
	filename string,
	taskId int,
	nReduce int,
) error {
	log.Printf("Starting Mapper for the worker\n")
	//open the file and read all the contents to the memory
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open file: %v, err: %v", filename, err)
	}
	content, err := ioutil.ReadAll(file)

	if err != nil {
		log.Fatalf("cannot read file: %v, err: %v", filename, err)
	}
	file.Close()
	log.Printf("Read the contents of the Map file: %v\n", filename)
	//remove the older files generated from the operation
	//removes for each map task mr-taskId-(0..nReduce]
	for i := 0; i < nReduce; i++ {
		oldTempFile := fmt.Sprintf("mr-%d-%d", taskId, i)
		err := os.Remove(filepath.Join(mapOutputDirName, oldTempFile))
		if err == nil {
			log.Printf("Deleted old tempFile, FileName= %v", oldTempFile)
		}
	}
	log.Printf("Deleted all the temporary files if any\n")
	keyValueArr := mapf(filename, string(content))

	log.Printf("Moving the Key Value Array partition into reduce tasks\n")
	/*
		Partition the kevValue Array for nReduce operations
		for each key it holds the KeyValue Array
	*/
	reduceKVArray := make(map[int][]mr.KeyValue)

	for _, val := range keyValueArr {
		reduceKey := int(ihash(val.Key)) % nReduce
		reduceKVArray[reduceKey] = append(reduceKVArray[reduceKey], val)
	}

	log.Printf(
		"Saving the partitioned KeyValue output into the files with mr-taskId-(0..nreduce-1)\n. " +
			"The data is encoded into json before saving it into the file\n",
	)
	/*
		Putting the Map % nReduce changes to reduce ready files.
	*/
	for i := 0; i < nReduce; i++ {
		outputFileName := fmt.Sprintf("mr-%d-%d", taskId, i)
		log.Printf("outputfile: %v\n", outputFileName)
		outputFile, err := os.OpenFile(
			filepath.Join(mapOutputDirName, outputFileName), os.O_RDWR|os.O_CREATE|os.O_EXCL, os.ModePerm,
		)
		if err != nil {
			log.Fatalf(
				"Failed to create output file for dir: %v and filename %v, err: %v", mapOutputDirName,
				outputFileName, err,
			)
		}

		encoder := json.NewEncoder(outputFile)
		for _, val := range reduceKVArray[i] {
			err := encoder.Encode(&val)
			if err != nil {
				log.Fatalf(
					"Cannot write to the outputdir: %v, outputfile: %v, err: %v", mapOutputDirName, outputFile, err,
				)
			}
		}

		outputFile.Close()
	}
	log.Printf("Completed the Mapper operation\n")
	return nil
}

func Reducer(reducef func(string, []string) string, taskId int, filename string) error {
	log.Printf("Starting Reduce operation for the task: %d", taskId)

	file, err := os.Open(filepath.Join(reduceDirPath, filename))
	if err != nil {
		log.Fatalf("cannot open file: %v, err: %v", filename, err)
	}

	log.Printf("Reading the contents for the reduce file: %s", filename)
	decoder := json.NewDecoder(file)
	keyValueArray := []mr.KeyValue{}
	for {
		var kv mr.KeyValue

		if err := decoder.Decode(&kv); err != nil {
			break
		}
		keyValueArray = append(keyValueArray, kv)
	}
	file.Close()

	outputFileName := fmt.Sprintf("mr-out-%d", taskId)

	//removing older files
	err = os.Remove(filepath.Join(outputDirName, outputFileName))
	if err == nil {
		log.Printf("Removed the old output file: %s in directory %s", outputFileName, outputDirName)
	}

	reduceOutputMap := make(map[string]string)
	for i := 0; i < len(keyValueArray); {
		j := i
		values := []string{}
		for ; j < len(keyValueArray) && keyValueArray[i].Key == keyValueArray[j].Key; {
			values = append(values, keyValueArray[j].Value)
			j++
		}
		output := reducef(keyValueArray[i].Key, values)
		reduceOutputMap[keyValueArray[i].Key] = output
		i = j
	}

	outputFile, err := os.OpenFile(
		filepath.Join(outputDirName, outputFileName), os.O_RDWR|os.O_CREATE|os.O_EXCL, os.ModePerm,
	)

	if err != nil {
		log.Fatalf(
			"Failed to create output file for dir: %v and filename %v, err: %v", outputDirName,
			outputFileName, err,
		)
	}

	for key, value := range reduceOutputMap {
		fmt.Fprintf(outputFile, "%v %v\n", key, value)
	}
	outputFile.Close()
	log.Printf("Reduce operation completed.")
	return nil
}

func Worker(
	mapf func(string, string) []mr.KeyValue,
	reducef func(string, []string) string,
) {
	err := os.Mkdir(mapOutputDirName, os.ModePerm)

	if err != nil {
		log.Printf("Failed to create temp directory: %v for processing, err: %v", mapOutputDirName, err)
	}

	log.Printf("Executing Map Tasks")
	for ; !checkForMapTasksCompletion(); {
		taskId, filename, nReduce := getMapTask()
		if taskId == -1 {
			log.Println("Didn't find any available Task")
			time.Sleep(1000 * time.Millisecond)
			continue
		}
		err := Mapper(mapf, filename, taskId, nReduce)
		if err == nil {
			updateMapTaskWithCompletion(taskId)
		}
		time.Sleep(1 * time.Second)
	}

	err = os.Mkdir(outputDirName, os.ModePerm)
	if err != nil {
		log.Printf("Failed to create output directory: %v for processing, err: %v", outputDirName, err)
	}

	log.Printf("Executing Reduce Tasks")
	for ; !checkForReduceTasksCompletion(); {
		taskId, filename := getReduceTask()
		if taskId == -1 {
			log.Println("Didn't find any available Task")
			time.Sleep(1000 * time.Millisecond)
			continue
		}
		err := Reducer(reducef, taskId, filename)
		if err == nil {
			updateReduceTaskWithCompletion(taskId)
		}
		time.Sleep(1 * time.Second)
	}
	log.Printf("All worker Map/Reduce tasks completed, stopping")

}

func call(api string, request interface{}, response interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	if err != nil {
		log.Fatalf("Failed with err: %v", err)
	}
	defer c.Close()

	err = c.Call(api, request, response)
	if err == nil {
		return true
	}
	log.Fatalf("Failed with err: %v", err)
	return false
}

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}
