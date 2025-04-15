package main

import (
	"fmt"
	"log"
	"sync"
	"time"
)

// Task represents a ride assignment task between a rider and a driver.
// The 'IsTerminationSignal' flag is used to indicate when a worker should stop.
type Task struct {
	Rider               string
	Driver              string
	IsTerminationSignal bool
}

// NewTask creates a normal ride assignment task.
func NewTask(rider, driver string) Task {
	return Task{
		Rider:               rider,
		Driver:              driver,
		IsTerminationSignal: false,
	}
}

// TerminationSignal returns a special task that signals the worker to stop.
func TerminationSignal() Task {
	return Task{
		IsTerminationSignal: true,
	}
}

// Process simulates handling a ride assignment task with a delay.
func (t Task) Process() {
	// Simulate computational work with a delay
	time.Sleep(1 * time.Second)
	fmt.Printf("Assigned %s to %s\n", t.Driver, t.Rider)
}

// worker is a goroutine that retrieves tasks from the channel and processes them.
// It writes results to the shared slice safely using a mutex.
func worker(id int, tasks <-chan Task, results *[]string, wg *sync.WaitGroup, mu *sync.Mutex) {
	defer wg.Done() // Decrement the WaitGroup counter when the worker exits

	for {
		task, ok := <-tasks // Receive task from the channel
		if !ok {
			log.Printf("Worker %d: task channel closed\n", id)
			return
		}

		// Stop processing if the termination signal is received
		if task.IsTerminationSignal {
			log.Printf("Worker %d: received termination signal\n", id)
			return
		}

		log.Printf("Worker %d: processing task for %s and %s\n", id, task.Rider, task.Driver)

		// Handle panic safely with defer and recover
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("Worker %d: error processing task: %v\n", id, r)
				}
			}()
			task.Process()
		}()

		// Use mutex to safely append to shared results slice
		mu.Lock()
		*results = append(*results, fmt.Sprintf("Assigned %s to %s", task.Driver, task.Rider))
		mu.Unlock()

		log.Printf("Worker %d: finished task\n", id)
	}
}

func main() {
	numWorkers := 4                         // Number of concurrent workers
	tasks := make(chan Task, 20)           // Buffered channel to hold tasks
	var results []string                   // Shared slice to store results
	var mu sync.Mutex                      // Mutex to guard shared results
	var wg sync.WaitGroup                  // WaitGroup to wait for all workers

	// Start worker goroutines
	for i := 1; i <= numWorkers; i++ {
		wg.Add(1)
		go worker(i, tasks, &results, &wg, &mu)
	}

	// Create and send ride tasks to the task channel
	for i := 1; i <= 10; i++ {
		tasks <- NewTask(fmt.Sprintf("Rider%d", i), fmt.Sprintf("Driver%d", i))
	}

	// Send a termination signal for each worker
	for i := 0; i < numWorkers; i++ {
		tasks <- TerminationSignal()
	}

	close(tasks)   // No more tasks will be added
	wg.Wait()      // Wait for all workers to complete

	// Print final assignment results
	fmt.Println("\nAll Assignments:")
	for _, r := range results {
		fmt.Println(r)
	}
}
