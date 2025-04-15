import java.util.concurrent.*;
import java.util.*;

// Main class to run the ride sharing simulation
public class Main {
    public static void main(String[] args) {
        SharedQueue queue = new SharedQueue();       // Shared queue between threads
        ResultLogger logger = new ResultLogger();    // Logger to store output
        int numWorkers = 4;                          // Number of worker threads
        ExecutorService pool = Executors.newFixedThreadPool(numWorkers); // Thread pool

        // Add tasks (simulated rides)
        for (int i = 0; i < 10; i++) {
            queue.addTask(new Task("Rider" + i, "Driver" + i));
        }

        // Add poison pills (special tasks to tell workers to stop)
        for (int i = 0; i < numWorkers; i++) {
            queue.addTask(Task.terminationSig());
        }

        // Submit worker threads to the executor service
        for (int i = 0; i < numWorkers; i++) {
            pool.submit(new Worker(queue, i, logger));
        }

        pool.shutdown(); // No more tasks to submit

        try {
            if (pool.awaitTermination(10, TimeUnit.SECONDS)) {
                logger.printResults(); // Print results after all workers are done
            } else {
                System.err.println("Timeout waiting for workers.");
            }
        } catch (InterruptedException e) {
            System.err.println("Interrupted while waiting for workers.");
        }
    }
}

// Represents an individual ride task between a driver and a rider
class Task {
    String rider;
    String driver;
    boolean isTerminationSig;

    // Constructor for regular task
    public Task(String rider, String driver) {
        this.rider = rider;
        this.driver = driver;
        this.isTerminationSig = false;
    }

    // Static method to create a termination signal ("poison pill")
    public static Task terminationSig() {
        Task pill = new Task("", "");
        pill.isTerminationSig = true;
        return pill;
    }

    public boolean isTerminationSig() {
        return isTerminationSig;
    }

    // Simulate task processing (e.g., driver assignment)
    public void process() throws InterruptedException {
        Thread.sleep(1000); // simulate 1 second of work
        System.out.println("Assigned " + driver + " to " + rider);
    }
}

// Thread-safe shared queue for tasks using BlockingQueue
class SharedQueue {
    private BlockingQueue<Task> queue = new LinkedBlockingQueue<>();

    // Add task to the queue (blocks if full)
    public void addTask(Task task) {
        try {
            queue.put(task);
        } catch (InterruptedException e) {
            System.err.println("Error adding task: " + e.getMessage());
        }
    }

    // Get task from the queue (blocks if empty)
    public Task getTask() {
        try {
            return queue.take();
        } catch (InterruptedException e) {
            System.err.println("Error getting task: " + e.getMessage());
            return null;
        }
    }
}

// Worker class that runs in a thread and processes tasks
class Worker implements Runnable {
    private SharedQueue queue;
    private int id;
    private ResultLogger logger;

    public Worker(SharedQueue queue, int id, ResultLogger logger) {
        this.queue = queue;
        this.id = id;
        this.logger = logger;
    }

    @Override
    public void run() {
        while (true) {
            Task task = queue.getTask();
            if (task == null) break;

            // Check if it's a termination signal
            if (task.isTerminationSig()) {
                System.out.println("Worker " + id + " received Termination Signal. Exiting...");
                break;
            }

            // Process task and log result
            try {
                System.out.println("Worker " + id + " started task.");
                task.process();
                String result = "Driver " + task.driver + " assigned to Rider " + task.rider;
                logger.log(result);
                System.out.println("Worker " + id + " completed task.");
            } catch (InterruptedException e) {
                System.err.println("Worker " + id + " error: " + e.getMessage());
            }
        }
    }
}

// Thread-safe logger to store and print ride assignment results
class ResultLogger {
    private List<String> results = Collections.synchronizedList(new ArrayList<>());

    // Add log entry (called by multiple threads)
    public void log(String entry) {
        results.add(entry);
    }

    // Print all collected results
    public void printResults() {
        System.out.println("\n--- Final Ride Assignments ---");
        for (String entry : results) {
            System.out.println(entry);
        }
    }
}
