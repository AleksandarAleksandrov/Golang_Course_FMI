package main

import (
	"sync"
)

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
	// create a wait group to sync the goroutines as to close
	// the resultsCh when all tasks are finished
	var wg sync.WaitGroup

	for index, task := range tasks {
		// add a task to the tasks channel
		// and add a counter to the wait group
		// execute a task from the channel concurrently
		tasksCh <- task
		wg.Add(1)
		go handleOneTask(&wg, tasksCh, task, index, 0, retryLimit, resultsCh)
	}

	// wait for all tasks to add their results to the resultsCh
	// and close the channel after that
	wg.Wait()
	close(resultsCh)
}

func handleOneTask(
	wg *sync.WaitGroup,
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
	if retryCount >= retryLimit {
		wg.Done()
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
		wg.Add(1)
		go handleOneTask(wg, tasksCh, task, taskIndex, retryCount+1, retryLimit, resultsCh)
		wg.Done()
		return
	}

	// if the task is ok remove it from the tasks channel
	<-tasksCh
	wg.Done()

}
