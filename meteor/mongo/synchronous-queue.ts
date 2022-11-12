import DoubleEndedQueue from "double-ended-queue";

type AsyncFunction = (...args: any[]) => Promise<any>;
type TaskHandle = { task: AsyncFunction, name: string, runTaskResolve?: Function, runTaskReject?: Function };

export class _SynchronousQueue {
    // List of tasks to run (not including a currently-running task if any). Each
    // is an object with field 'task' (the task function to run) and 'future' (the
    // Future associated with the blocking runTask call that queued it, or null if
    // called from queueTask).
    private _taskHandles = new DoubleEndedQueue<TaskHandle>();

    // This is true if self._run() is either currently executing or scheduled to
    // do so soon.
    private _runningOrRunScheduled = false;

    // This is true if we're currently draining.  While we're draining, a further
    // drain is a noop, to prevent infinite loops.  "drain" is a heuristic type
    // operation, that has a meaning like unto "what a naive person would expect
    // when modifying a table from an observe"
    private _draining = false;

    constructor() { }

    async runTask(task: AsyncFunction) {
        return new Promise((resolve, reject) => {
            var handle: TaskHandle = {
                task: task,
                name: task.name,
                runTaskResolve: resolve,
                runTaskReject: reject
            };
            this._taskHandles.push(handle);
            this._scheduleRun();
            // Yield. We'll get back here after the task is run (and will throw if the
            // task throws).
                
        })
    }

    queueTask(task: AsyncFunction) {
        var self = this;
        self._taskHandles.push({
            task: task,
            name: task.name
        });
        self._scheduleRun();
        // No need to block.
    }

    async flush() {
        var self = this;
        await self.runTask(async () => { });
    }

    async drain() {
        var self = this;
        if (self._draining)
            return;
        self._draining = true;
        while (!self._taskHandles.isEmpty()) {
            await self.flush();
        }
        self._draining = false;
    };

    _scheduleRun() {
        // Already running or scheduled? Do nothing.
        if (this._runningOrRunScheduled)
            return;

        this._runningOrRunScheduled = true;
        setImmediate(async () => {
            await this._run();
        });
    };

    async _run() {
        var self = this;

        if (!self._runningOrRunScheduled)
            throw new Error("expected to be _runningOrRunScheduled");

        if (self._taskHandles.isEmpty()) {
            // Done running tasks! Don't immediately schedule another run, but
            // allow future tasks to do so.
            self._runningOrRunScheduled = false;
            return;
        }
        var taskHandle = self._taskHandles.shift();

        // Run the task.
        var exception = undefined;
        try {
            await taskHandle.task();
        } catch (err) {
            if (taskHandle.runTaskReject) {
                // We'll throw this exception through runTask.
                exception = err;
            } else {
                console.error("Exception in queued task", err);
            }
        }

        // Soon, run the next task, if there is any.
        self._runningOrRunScheduled = false;
        self._scheduleRun();

        // If this was queued with runTask, let the runTask call return (throwing if
        // the task threw).
        if (taskHandle.runTaskReject) {
            if (exception)
                taskHandle.runTaskReject(exception);
            else
                taskHandle.runTaskResolve();
        }
    }

}