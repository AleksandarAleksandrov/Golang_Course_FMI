package main

func ConcurrentRetryExecutor(tasks []func() string, concurrentLimit int, retryLimit int) <-chan struct {
	index  int
	result string
} {
	// create the results channel
	resultsCh := make(chan struct {
		index  int
		result string
	}, concurrentLimit)
	// concurrently handle all the tasks
	go handleTasks(tasks, concurrentLimit, retryLimit, resultsCh)
	return resultsCh
}
func handleTasks(
	tasks []func() string,
	concurrentLimit int,
	retryLimit int,
	resultsCh chan struct {
		index  int
		result string
	}) {
	// create a limited size channel for the tasks
	tasksCh := make(chan func() string, concurrentLimit)
	// create a channel to use for synchronisation
	syncCh := make(chan struct{}, len(tasks))
	for index, task := range tasks {
		// add a task to the tasks channel
		// execute a task from the channel concurrently
		tasksCh <- task
		go handleOneTask(syncCh, tasksCh, task, index, 0, retryLimit, resultsCh)
	}
	// depending on the size of the tasks slice
	// remove from syncCh channel until all tasks
	// are removed and close the channel
	for i := 0; i < len(tasks); i++ {
		<-syncCh
	}
	close(resultsCh)
}
func handleOneTask(
	syncCh chan struct{},
	tasksCh chan func() string,
	task func() string,
	taskIndex int,
	retryCount int,
	retryLimit int,
	resultsCh chan struct {
		index  int
		result string
	}) {
	// stop retrying this task
	// remove it from the tasks channel
	// and add a struct to the syncCh
	if retryCount >= retryLimit {
		<-tasksCh
		syncCh <- struct{}{}
		return
	}
	// get the task's result and send it to the channel
	result := task()
	resultsCh <- struct {
		index  int
		result string
	}{index: taskIndex, result: result}
	// if the result is "" retry the task
	if result == "" {
		go handleOneTask(syncCh, tasksCh, task, taskIndex, retryCount+1, retryLimit, resultsCh)
		return
	}
	// if the task is ok remove it from the tasks channel
	<-tasksCh
	syncCh <- struct{}{}
}
