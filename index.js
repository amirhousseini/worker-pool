/**
 * Module providing a custom implementation of a worker pool with helper functions.
 */

'use strict';

const dayjs = require('dayjs');
const { stdout } = require('node:process');
const util = require('node:util');
const { cpus, EOL } = require('node:os');
const { Worker, threadId, parentPort } = require('node:worker_threads');
const { handleOptionalArgs } = require('ah-toolbox');

// Message types of messages exchanged between threads
// Message sent from main thread to children threads
const taskMT = "task";
const stopMT = "stop";
// Message sent from children threads to main thread
const resultMT = "result";
const ackMT = "ack";

/**
 * Class providing worker-scoped logging to the console.
 * @param {...any} data Data to be logged.
 */
class ThreadLogger {
    /**
     * Construct a ThreadLogger.
     * @param {Stream} out Output stream where logs will ne written.
     *  If undefined defaults to process.stdout.
     */
    constructor(out) {
        this.out = stdout;
        if (out !== undefined) this.out = out;
    }

    /**
     * Write to the output stream the data passed as argument, formatted using process:util.format(),
     * newlines being replaced by single space characters.
     * @param  {...any} data Data to be logged.
     */
    log(...data) {
        const timestamp = dayjs().format();
        const origin = threadId === 0 ? "[Master]" : `[Worker #${threadId}]`;
        const text = data.reduce((prev, curr) => {
            let item = util.format(curr);
            if (typeof curr === "object") item = item.replace(/\s+/g, ' ');
            return prev += item + ' '
        }, "");
        this.out.write(`${timestamp} ${origin} ${text} ${EOL}`);
    }
}

const logger = new ThreadLogger();

/**
 * Post a message to a recipient.
 * If the sender is the main thread the recipient is a Worker.
 * If the sender is a child thread the recipient is the parent port.
 * A message has the following structure:
 * { type: <type>, sender: <threadId>, payload: <payload> }
 * @param {Port|Worker} recipient The destination of the message.
 * @param {string} type The message type.
 * @param {any} payload The message payload. Typically a compound object.
 * @returns The message object that has been posted
 */
function postMessage(recipient, type, payload) {
    const msg = { type };
    if (threadId > 0) msg.sender = threadId;
    if (payload !== undefined) msg.payload = payload;
    recipient.postMessage(msg);
    /*
    if (recipient.threadId === undefined) {
        logger.log("Sent to #0:", msg);
    } else {
        logger.log(`Sent to #${recipient.threadId}:`, msg);
    }
    */
    return msg;
}

/**
 * Generator of task input objects having the sole property taskId, an integer
 * in a given range.
 * @param {number} start First and lowest value for taskId
 * @param {number} length Number of taskId generated.
 * @return as value the object { taskId }.
 */
function* simpleTaskIdGenerator(start, length) {
    for (let taskId = start; taskId <start + length; taskId++){
        yield { taskId };
    }
}

/**
 * Class managing a pool of worker threads and the message protocol between main
 * and children threads.
 */
class WorkerPool {

    // Workers lists. A worker is either in the ready or busy lists.
    #readyWorkers = [];
    #busyWorkers = [];
    // Map of worker Id to promise which fulfills to events from workers, specially messages
    #workerEvents = new Map();
    
    /**
     * Construct a WorkerPool with the workers' main script or module and optional initialization data. 
     * @param {string|URL} filename Optional path to the workers' main script or module; defaults to the entry script when the Node.js process launched.
     * @param {Object} data Optional worker data common to all workers.
     * @param {number} size Optional number of worker threads; defaults to the number of CPU cores.
     */
    constructor(filename, data, size) {
        [filename, data, size] = handleOptionalArgs(
            arguments,
            ["string", "object", "number"],
            [require.main.filename, {}, cpus().length]);
        // Create all workers
        const options = data ? { workerData: data } : undefined;
        for (let i = 0; i < size; i++) {
            this.#readyWorkers.push(new Worker(filename, options));
        }
        logger.log("Worker pool with", size, "workers created.");
    }

    /*
     * Activate a free worker by sending a message.
     * @param {string} messageType The message type.
     * @param {Object} messagePayload The message payload, i.e. task input data.
     * @returns the activated worker.
     */
    #activateOneWorker(messageType, messagePayload) {
        const worker = this.#readyWorkers.shift();
        if (worker) {
            // Move the worker from the ready list to the busy list
            this.#busyWorkers.push(worker);
            // Listen to the worker events
            this.#workerEvents.set(worker.threadId, new Promise((resolve, reject) => {
                worker.removeAllListeners()
                .on("message", resolve)
                .on("error", reject)
                .on("exit", (code) => {
                    // Do nothing if exit status code is 0
                    if (code !== 0)
                        reject(new Error(`Worker #${worker.threadId} stopped with exit code ${code}`));
                });
            }));
            // Post the specified message to the worker
            postMessage(worker, messageType, messagePayload);
            //logger.log(`Worker #${worker.threadId} activated.`);
        }
        return worker;
    }

    /*
     * Inactivate the worker referenced by it's unique thread Id
     * @param {any} threadId The worker's unique thread Id.
     */
    #inactivateWorker(threadId) {
        const index = this.#busyWorkers.findIndex((worker) => worker.threadId === threadId);
        if (index >= 0) {
            const worker = this.#busyWorkers.splice(index, 1)[0];
            this.#readyWorkers.push(worker);
        }
        this.#workerEvents.delete(threadId);
        //logger.log(`Worker #${threadId} inactivated.`);
    }

    /*
     * Boolean function returning whether there is a free workr available.
     * @returns Boolean
     */
    #isAnyWorkerAvailable() {
        return this.#readyWorkers.length > 0;
    }

    /*
     * Return the next available task result.
     * @returns Promise that fulfills with the next available task output data.
     */
    async #nextTaskResult() {
        const { sender, payload } = await Promise.race(this.#workerEvents.values());
        this.#inactivateWorker(sender);
        return payload;
    }

    /**
     * Dispatch tasks to workers and return results as a generator.
     * @param {function} tasks Generator of task input data.
     * @returns Promise that fulfills with a generator of tasks' output data.
     */
    async * dispatchTasks(tasks) {
        // Dispatch tasks to workers as long as there are workers and tasks
        do {
            while (this.#isAnyWorkerAvailable()) {
                const { value, done } = await tasks.next();
                if (done) break;
                this.#activateOneWorker(taskMT, value);
            }
            yield await this.#nextTaskResult();
        } while (this.#workerEvents.size > 0);
        // All tasks executed at this point
    
        // Stop all workers
        while (this.#isAnyWorkerAvailable()) {
            this.#activateOneWorker(stopMT);
        }
        await Promise.all(this.#workerEvents.values());
        // All workers stopped at that point
    }

    /*
    // This does not work for some reason...
    async executeTasks(tasks, execute, terminate) {
        const results = this.dispatchTasks(tasks);
        let count = 0;
        while (true) {
            const { value, done } = await results.next();
            if (done) {
                if (terminate) await terminate();
                return count;
            }
            await execute(value);
            count++;
        }    
    }
    */
}

/**
 * Helper function to process results from workers.
 * @param {function*} results Generator of task results
 * @param {function} execute Callback function handling the processing of a task's result.
 *  Argument is the task's output data.
 * @param {function} terminate Callback function called before the main thread stops.
 *  It has no arguments. This is a hook for closing resources, and cleaning up.
 * @returns A promise that fulfills with the total count of worker results processed.
 */
async function processResults(results, execute, terminate) {
    let count = 0;
    while (true) {
        const { value, done } = await results.next();
        if (done) {
            if (terminate) await terminate();
            return count;
        }
        await execute(value);
        count++;
    }
}

/**
 * Driver function called by workers to process requests from the main thread.
 * @param {function} execute Callback function handling the execution of a task.
 *  Argument is the task's input data and return value is the tasks's output data.
 * @param {function} terminate Callback function called whenever a worker is stopped.
 *  It has no arguments. It is a hook for closing resources, and cleaning up.
 */
async function processRequests(execute, terminate) {
    // Listen to messages from the main thread
    parentPort.on('message', (message) => {
        //logger.log("Received:", msg);
        if (message.type === taskMT) {
            execute(message.payload)
            .then((result) => postMessage(parentPort, resultMT, result));
        } else if (message.type === stopMT) {
            try {
                if (terminate) terminate();
            } finally {
                // Acknowledge to parent
                postMessage(parentPort, ackMT);
                //logger.log("Terminated.")
            }
        } else {
            throw new Error(`Unknown message type ${message.type} received.`);
        }
    });
}

module.exports = {
    ThreadLogger, WorkerPool, simpleTaskIdGenerator, processRequests, processResults
}
