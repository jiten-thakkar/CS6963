package mapreduce

import "container/list"
import (
	"fmt"
	"log"
)


type WorkerInfo struct {
	address string
	// You can add definitions here.
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	availWorkersQueue := AvailWorkers{}
	availWorkersQueue.initAvailWorkers()
	mapJobsDone := make(chan bool)
	reduceJobsDone := make(chan bool)
	mapJobsDoneChannel := make(chan DoJobArgs, mr.nMap)
	mapTobeDoneChannel := make(chan DoJobArgs, mr.nMap)
	reduceJobsTobeDoneChannel := make(chan DoJobArgs, mr.nReduce)
	reduceJobsDoneChannel := make(chan DoJobArgs, mr.nReduce)
	for i := 0; i < mr.nMap; i++ {
		job := DoJobArgs{}
		job.Operation = Map
		job.File = mr.file
		job.JobNumber = i
		job.NumOtherPhase = mr.nReduce
		mapTobeDoneChannel <- job
	}
//	close(mapTobeDoneChannel)
	log.Println("maptobedonechannel len: ", len(mapTobeDoneChannel))
	go func() {
		for v := range mr.registerChannel {
			availWorkersQueue.addWorker(v)
		}
	}()

	go func() {
		for v := range mapTobeDoneChannel {

			//			DoMap(i, mr.file, mr.nReduce, Map)
			tempJobParams := v
			go func() {
				var availWorkerAddress string
				availWorkerAddress = availWorkersQueue.getWorker()
				rep := DoJobReply{}
				ok := call(availWorkerAddress, "Worker.DoJob", &tempJobParams, &rep)
				if ok == true {
					//fmt.Println("map job done job number: ", tempJobParams.JobNumber)
					mapJobsDoneChannel <- v
					availWorkersQueue.addWorker(availWorkerAddress)
					//fmt.Println("cap: %v len: %v nmap: %v", cap(reduceJobsDoneChannel), len(reduceJobsDoneChannel), mr.nMap)
					if len(mapJobsDoneChannel) == mr.nMap {
						log.Println("map jobs done")
						mapJobsDone <- true
					}
				} else {
					//fmt.Println("map job failed job number: ", v.JobNumber)
					//fmt.Println("error: ", ok)
					mapTobeDoneChannel <- tempJobParams
				}
			}()
		}
	}()

	<-mapJobsDone

	for i := 0; i < mr.nReduce; i++ {
		//			DoReduce(i, mr.file, mr.nMap, Reduce)
		job := DoJobArgs{}
		job.Operation = Reduce
		job.File = mr.file
		job.JobNumber = i
		job.NumOtherPhase = mr.nMap
		reduceJobsTobeDoneChannel <- job
	}

	go func() {
		for v := range reduceJobsTobeDoneChannel {
			//log.Println("starting reduce job for number: ", v.JobNumber)
			tempJobParams := v
			go func() {
				var availWorkerAddress string
				availWorkerAddress = availWorkersQueue.getWorker()
				rep := DoJobReply{}
				ok := call(availWorkerAddress, "Worker.DoJob", &tempJobParams, &rep)
				if ok == true {
					//log.Println("reduce job done job number: ", tempJobParams.JobNumber)
					reduceJobsDoneChannel <- v
					availWorkersQueue.addWorker(availWorkerAddress)
					//fmt.Println("len: %v nmap: %v", len(reduceJobsDoneChannel), mr.nReduce)
					if len(reduceJobsDoneChannel) == mr.nReduce {
						reduceJobsDone <- true
					}
				} else {
					//log.Println("reduce job failed job number: ", v.JobNumber)
					//log.Println("error: ", ok)
					reduceJobsTobeDoneChannel <- tempJobParams
				}
		}()
		}
	}()

	<-reduceJobsDone
	// Your code here
	return mr.KillWorkers()
}
