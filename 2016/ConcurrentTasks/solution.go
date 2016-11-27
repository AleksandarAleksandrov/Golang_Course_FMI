package main

import (
	"errors"
	"sync"
	"time"
)

type Task interface {
	Execute(int) (int, error)
}

type pipelineTask struct {
	tasks []Task
}

func (pt *pipelineTask) Execute(passed int) (int, error) {
	tasks := pt.tasks
	// check the tasks slice
	if tasks == nil || len(tasks) == 0 {
		return 0, errors.New("Received nil or zero arguments for pipeline.")
	}
	var err error = nil
	// for each task pass the answer from the last and check for an error
	for _, task := range tasks {
		passed, err = task.Execute(passed)
		if err != nil {
			return 0, err
		}
	}
	return passed, nil
}

func Pipeline(tasks ...Task) Task {
	var pt Task = &pipelineTask{tasks: tasks}
	return pt
}

type fastestTask struct {
	tasks []Task
}

// create a struct to be used in the channel of results
type result struct {
	answer int
	err    error
}

func (ft *fastestTask) Execute(passed int) (int, error) {
	tasks := ft.tasks
	// check the tasks slice
	if tasks == nil || len(tasks) == 0 {
		return 0, errors.New("Received nil or zero arguments for fastest task.")
	}

	// make a channel of results for the tasks
	resultCh := make(chan result, len(tasks))
	for _, task := range tasks {
		// execute each task concurrently and write the result to the channel
		go func(task Task) {
			answer, err := task.Execute(passed)
			res := result{answer: answer, err: err}
			resultCh <- res
		}(task)
	}

	// get the first finished result
	firstRes := <-resultCh

	//spawn a routine to get remaining result and close the channel
	go func() {
		for i := 0; i < len(tasks)-1; i++ {
			<-resultCh
		}
		close(resultCh)
	}()

	return firstRes.answer, firstRes.err

}

func Fastest(tasks ...Task) Task {
	var ft Task = &fastestTask{tasks: tasks}
	return ft
}

type timedTask struct {
	task    Task
	timeout time.Duration
}

func (tt *timedTask) Execute(passed int) (int, error) {
	// make a channel for the task execution
	resultCh := make(chan result, 1)
	// execute the task concurrently and pass the result to the channel
	go func(task Task) {
		answer, err := task.Execute(passed)
		resultCh <- result{answer: answer, err: err}
	}(tt.task)
	// chose the first result
	select {
	case res := <-resultCh:
		// close the channel after use
		close(resultCh)
		return res.answer, res.err
	case <-time.After(tt.timeout):
		// spwan a routine to read from the channel and close it after that
		go func() {
			<-resultCh
			close(resultCh)
		}()
		return 0, errors.New("Task did not finish in required timeframe.")
	}
}

func Timed(task Task, timeout time.Duration) Task {
	var tt Task = &timedTask{task: task, timeout: timeout}
	return tt
}

type cmrTask struct {
	reduce func([]int) int
	tasks  []Task
}

func (t *cmrTask) Execute(passed int) (int, error) {
	tasks := t.tasks
	// check the tasks slice
	if tasks == nil || len(tasks) == 0 {
		return 0, errors.New("Received nil or zero arguments for concurrent map reduce.")
	}

	// channel for the errors
	errCh := make(chan error, len(tasks))
	// channel for the results
	resultCh := make(chan int, len(tasks))
	// channel for synchronisation
	syncCh := make(chan struct{}, 1)

	// spawn a routine that executes all tasks concurrently and waits
	// for them to finish
	go func(tasks []Task) {
		var wg sync.WaitGroup
		for _, task := range tasks {
			wg.Add(1)
			// execute each task concurrently
			go func(task Task) {
				answer, err := task.Execute(passed)
				// if there was an error add it to the errors channel
				if err != nil {
					errCh <- err
				} else {
					resultCh <- answer
				}
				wg.Done()
			}(task)
		}

		wg.Wait()
		close(resultCh)
		// when all tasks are finished add to the synch channel
		syncCh <- struct{}{}
		close(syncCh)
	}(tasks)

	// wait for all tasks to finish or there is an error
	select {
	case <-syncCh:
		results := make([]int, 0)
		for {
			res, ok := <-resultCh
			if !ok {
				break
			}
			results = append(results, res)
		}
		return t.reduce(results), nil
	case err := <-errCh:
		return 0, err
	}

}

func ConcurrentMapReduce(reduce func(results []int) int, tasks ...Task) Task {
	var cmpt Task = &cmrTask{tasks: tasks, reduce: reduce}
	return cmpt
}

type gsTask struct {
	tasks      <-chan Task
	errorLimit int
}

func (t *gsTask) Execute(passed int) (int, error) {
	results := make([]int, 0)
	tasks := t.tasks
	errorLimit := 0
	// variables used for synchronisation
	var mutext sync.Mutex
	var wg sync.WaitGroup

	// while the channel is open execute each task concurrently
	for {
		task, ok := <-tasks
		if !ok {
			break
		}

		wg.Add(1)
		go func(task Task) {
			answer, err := task.Execute(passed)
			// depending on the result use the mutex to increment one of the two
			if err != nil {
				mutext.Lock()
				errorLimit++
				mutext.Unlock()
			} else {
				mutext.Lock()
				results = append(results, answer)
				mutext.Unlock()
			}
			wg.Done()
		}(task)

	}

	// wait for all tasks to finish
	wg.Wait()

	if errorLimit > t.errorLimit {
		return 0, errors.New("Error limit exceeded.")
	}

	if len(results) == 0 {
		return 0, errors.New("No results were processed.")
	}

	biggest := results[0]
	for i := 1; i < len(results); i++ {
		if biggest < results[i] {
			biggest = results[i]
		}
	}

	return biggest, nil
}

func GreatestSearcher(errorLimit int, tasks <-chan Task) Task {
	var gst Task = &gsTask{tasks: tasks, errorLimit: errorLimit}
	return gst
}
