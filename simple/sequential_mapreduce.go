package simple

import (
	"fmt"
	"gomr.com/gomr/mr"
	"gomr.com/gomr/utils"
	"io/ioutil"
	"log"
	"os"
	"sort"
)

func SimpleMapReduce()  {
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "Usage: gomr xxx.go inputFiles")
		os.Exit(1)
	}

	exec_file := os.Args[1]

	mapf, reducef := utils.LoadPlugin(exec_file)

	intermediate := []mr.KeyValue{}

	for _, filename := range os.Args[2:] {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open file: %v", filename)
		}
		content, err := ioutil.ReadAll(file)

		if err != nil {
			log.Fatalf("cannot read file: %v", filename)
		}

		file.Close()

		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}

	sort.Sort(mr.SortKey(intermediate))

	oname := "mr-out-0"
	ofile, _ := os.Create(oname)

	/**
	Calling Reduce on each distinct key
	*/

	for i := 0; i < len(intermediate); {
		j := i + 1
		for ; j < len(intermediate) && intermediate[i].Key == intermediate[j].Key; j++ {

		}
		values := []string{}
		for _, keyvalue := range intermediate[i:j] {
			values = append(values, keyvalue.Value)
		}

		output := reducef(intermediate[i].Key, values)

		fmt.Fprintf(ofile, "%v %v \n", intermediate[i].Key, output)
		i = j
	}
	ofile.Close()
}
