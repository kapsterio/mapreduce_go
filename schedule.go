package mapreduce

import "fmt"

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	//var taskArr []int
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	task := make(chan int)
	go func() {
		for index := 0; index < ntasks; index++ {
			task <- index
		}
	}()

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	for id := range task {
		workerAddr := <-mr.registerChannel
		var args DoTaskArgs
		args.File = mr.files[id]
		args.JobName = mr.jobName
		args.NumOtherPhase = nios
		args.Phase = phase
		args.TaskNumber = id

		go func() {
			suc := call(workerAddr, "Worker.DoTask", &args, new(struct{}))
			if suc {
				if args.TaskNumber == (ntasks - 1) {
					close(task)
				}
				mr.registerChannel <- workerAddr
			} else {
				task <- args.TaskNumber

			}
		}()
	}
	// drain the registerChannel
	/**
	if phase == reducePhase {
		for range mr.workers {
			<-mr.registerChannel
		}
	}
	*/
	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	fmt.Printf("Schedule: %v phase done\n", phase)
}
