package main

import (
	"fmt"
	"gomr.com/gomr/distributed"
	"gomr.com/gomr/utils"
	"log"
	"os"
	"time"
)

type Command string

const (
	Controller Command = "controller"
	Worker     Command = "worker"
)

func processController() {
	log.Print("Starting the Controller")
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: gomr controller input-files")
		os.Exit(1)
	}
	c := distributed.MakerController(os.Args[2:], 10)
	for !c.Done() {
		log.Printf("Waiting for the Map Task to Complete")
		time.Sleep(5 * time.Second)
	}
	time.Sleep(1 * time.Second)
	log.Printf("All tasks completed! Shutting down master")
}

func processWorker() {
	log.Print("Starting the worker")
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "Usage: gomr worker xxx.so")
		os.Exit(1)
	}

	exec_file := os.Args[2]

	mapf, reducef := utils.LoadPlugin(exec_file)
	distributed.Worker(mapf, reducef)
}

func main() {
	//simple.SimpleMapReduce()
	switch command := Command(os.Args[1]); command {
	case Controller:
		processController()

	case Worker:
		processWorker()

	default:
		log.Fatal("Wrong Command user gomr Controller or gomr Worker")
	}
}
