package mapreduce

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
)

// Holds the state for a server waiting for DoTask or Shutdown RPCs
type Worker struct {
	sync.Mutex

	name   string
	Map    func(string, string) []KeyValue
	Reduce func(string, []string) string

	nRPC       int // Quit after this many RPCs; Protected by mutex
	nTasks     int // Total tasks executed; Protected by mutex
	concurrent int // Number of parallel DoTasks in this worker; mutex

	l net.Listener
}

// Called by the master when a new task is being scheduled on this worker
func (wk *Worker) DoTask(arg *DoTaskArgs, _ *struct{}) error {
	fmt.Printf("%s: given %v task #%d on file %s (nios: %d)\n",
		wk.name, arg.Phase, arg.TaskNumber, arg.File, arg.NumOtherPhase)

	wk.Lock()
	wk.nTasks += 1
	wk.concurrent += 1
	nc := wk.concurrent
	wk.Unlock()

	if nc > 1 {
		// schedule() should never issue more than one RPC at a time to a given worker.
		log.Fatal("Worker.DoTask: more than one DoTask sent concurrently to a single worker\n")
	}

	switch arg.Phase {
	case mapPhase:
		doMap(arg.JobName, arg.TaskNumber, arg.File, arg.NumOtherPhase, wk.Map)
	case reducePhase:
		doReduce(arg.JobName, arg.TaskNumber, mergeName(arg.JobName, arg.TaskNumber), arg.NumOtherPhase, wk.Reduce)
	}

	wk.Lock()
	wk.concurrent -= 1
	wk.Unlock()

	fmt.Printf("%s: %v task #%d done\n", wk.name, arg.Phase, arg.TaskNumber)
	return nil
}

// Shutdown is called by the master when all work has been completed.
// Responds with the number of tasks processed by it
func (wk *Worker) Shutdown(_ *struct{}, res *ShutdownReply) error {
	debug("Shutdown %s\n", wk.name)

	wk.Lock()
	defer wk.Unlock()

	res.NTasks = wk.nTasks
	wk.nRPC = 1

	return nil
}

// Tell the master that the worked is ready to do work
func (wk *Worker) register(master string) {
	args := new(RegisterArgs)
	args.Worker = wk.name

	ok := call(master, "Master.Register", args, new(struct{}))
	if !ok {
		fmt.Printf("Register: RPC %s register error\n", master)
	}
}

// Sets up a connection with the master, registers its address, and waits for tasks to be scheduled
func RunWorker(MasterAddress string, me string,
	MapFunc func(string, string) []KeyValue,
	ReduceFunc func(string, []string) string,
	nRPC int,
) {
	debug("RunWorker %s\n", me)

	wk := new(Worker)
	wk.name = me
	wk.Map = MapFunc
	wk.Reduce = ReduceFunc
	wk.nRPC = nRPC

	rpcs := rpc.NewServer()
	rpcs.Register(wk)

	os.Remove(me) // Only needed for "unix"
	l, e := net.Listen("unix", me)
	if e != nil {
		log.Fatal("RunWorker: worker ", me, " error: ", e)
	}
	wk.l = l
	wk.register(MasterAddress)

	for {
		wk.Lock()
		if wk.nRPC == 0 {
			wk.Unlock()
			break
		}

		wk.Unlock()
		conn, err := wk.l.Accept()
		if err == nil {
			wk.Lock()
			wk.nRPC--
			wk.Unlock()
			go rpcs.ServeConn(conn)
		} else {
			break
		}
	}

	wk.l.Close()
	debug("RunWorker %s exit\n", me)
}
