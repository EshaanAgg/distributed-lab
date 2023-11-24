package mapreduce

import "fmt"

// It starts and waits for all tasks in the given phase (Map or Reduce).
// `mapFiles` holds the names of the files that are the inputs to the map phase, one per map task.
// `nReduce“ is the number of reduce tasks.
// `registerChan` yields a stream of registered workers; each item is the worker's RPC address
// It will yield all existing registered workers (if any) and new ones as they register.
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var nTasks int
	var nOther int // Number of inputs (for reduce) or outputs (for map)

	switch phase {
	case mapPhase:
		nTasks = len(mapFiles)
		nOther = nReduce
	case reducePhase:
		nTasks = nReduce
		nOther = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", nTasks, phase, nOther)

	// A helper function to create `DoTaskArgs` struct
	constructTaskArgs := func(phase jobPhase, task int) DoTaskArgs {
		var taskArgs DoTaskArgs

		taskArgs.Phase = phase
		taskArgs.JobName = jobName
		taskArgs.NumOtherPhase = nOther
		taskArgs.TaskNumber = task
		if phase == mapPhase {
			taskArgs.File = mapFiles[task]
		}
		return taskArgs
	}

	// Asks like a task queue, keeping track of the tasks that are yet to be scheduled
	tasks := make(chan int)
	go func() {
		for i := 0; i < nTasks; i++ {
			tasks <- i
		}
	}()

	// Keeps track of the number of tasks that have been completed successfully
	successTasks := 0
	loop := true

	for loop {
		select {
		case task := <-tasks:
			go func() {
				worker := <-registerChan
				status := call(worker, "Worker.DoTask", constructTaskArgs(phase, task), nil)
				if status {
					successTasks++
					registerChan <- worker
				} else {
					tasks <- task
				}
			}()

		default:
			if successTasks == nTasks {
				close(tasks)
				loop = false
			}
		}
	}

	fmt.Printf("Schedule: %v phase done\n", phase)
}
