package mapreduce

//import (
//	"container/list"
//)

type AvailWorkers struct {
	workersChannel chan string
//	workers map[string]*WorkerInfo
	Err string
}

func (aw *AvailWorkers) Error() string {
	return aw.Err
}

func (aw *AvailWorkers) getWorker() string {
		return <-aw.workersChannel
}

func (aw *AvailWorkers) addWorker(wi string) error {
	aw.workersChannel <- wi
	return nil
}

func (aw *AvailWorkers) initAvailWorkers() error {
	aw.workersChannel = make(chan string, 100000)
	return nil
}
