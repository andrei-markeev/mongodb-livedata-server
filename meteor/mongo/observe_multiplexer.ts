import { clone } from "../ejson/ejson";
import { _CachingChangeObserver } from "./caching_change_observer";
import { _SynchronousQueue } from "./synchronous-queue";

export class ObserveMultiplexer {
    private _ordered: boolean;
    private _onStop: Function;
    private _queue: _SynchronousQueue;
    private _handles: Record<number, ObserveHandle>;
    private _readyFuture: { promise?: Promise<void>, resolve?: Function, reject?: Function, isResolved: boolean } = { isResolved: false };
    private _cache: any;
    private _addHandleTasksScheduledButNotPerformed: number;

    public added: (id: string, fields: Record<string, any>) => void;
    public changed: (id: string, fields: Record<string, any>) => void;
    public removed: (id: string) => void;

    constructor(options) {
        var self = this;

        if (!options || !options.hasOwnProperty('ordered'))
            throw Error("must specify ordered");

        self._ordered = options.ordered;
        self._onStop = options.onStop || function () { };
        self._queue = new _SynchronousQueue();
        self._handles = {};
        self._readyFuture.promise = new Promise<void>((resolve, reject) => {
            self._readyFuture.resolve = resolve;
            self._readyFuture.reject = reject;
        });
        self._cache = new _CachingChangeObserver({ ordered: options.ordered });
        // Number of addHandleAndSendInitialAdds tasks scheduled but not yet
        // running. removeHandle uses this to know if it's time to call the onStop
        // callback.
        self._addHandleTasksScheduledButNotPerformed = 0;

        for (const callbackName of self.callbackNames()) {
            self[callbackName] = function (/* ... */) {
                self._applyCallback(callbackName, Array.from(arguments));
            };
        }
    }

    async addHandleAndSendInitialAdds(handle: ObserveHandle) {
        var self = this;

        // Check this before calling runTask (even though runTask does the same
        // check) so that we don't leak an ObserveMultiplexer on error by
        // incrementing _addHandleTasksScheduledButNotPerformed and never
        // decrementing it.
        //if (!self._queue.safeToRunTask())
        //    throw new Error("Can't call observeChanges from an observe callback on the same query");
        ++self._addHandleTasksScheduledButNotPerformed;

        await self._queue.runTask(async () => {
            self._handles[handle._id] = handle;
            // Send out whatever adds we have so far (whether or not we the
            // multiplexer is ready).
            self._sendAdds(handle);
            --self._addHandleTasksScheduledButNotPerformed;
        });
        // *outside* the task, since otherwise we'd deadlock
        await self._readyFuture.promise;
    }

    // Remove an observe handle. If it was the last observe handle, call the
    // onStop callback; you cannot add any more observe handles after this.
    //
    // This is not synchronized with polls and handle additions: this means that
    // you can safely call it from within an observe callback, but it also means
    // that we have to be careful when we iterate over _handles.
    removeHandle(id: number) {
        var self = this;

        // This should not be possible: you can only call removeHandle by having
        // access to the ObserveHandle, which isn't returned to user code until the
        // multiplex is ready.
        if (!self._ready())
            throw new Error("Can't remove handles until the multiplex is ready");

        delete self._handles[id];

        if (Object.keys(self._handles).length === 0 && self._addHandleTasksScheduledButNotPerformed === 0) {
            self._stop();
        }
    }
    _stop(options?) {
        var self = this;
        options = options || {};

        // It shouldn't be possible for us to stop when all our handles still
        // haven't been returned from observeChanges!
        if (!self._ready() && !options.fromQueryError)
            throw Error("surprising _stop: not ready");

        // Call stop callback (which kills the underlying process which sends us
        // callbacks and removes us from the connection's dictionary).
        self._onStop();

        // Cause future addHandleAndSendInitialAdds calls to throw (but the onStop
        // callback should make our connection forget about us).
        self._handles = null;
    }

    // Allows all addHandleAndSendInitialAdds calls to return, once all preceding
    // adds have been processed. Does not block.
    ready() {
        var self = this;
        self._queue.queueTask(async () => {
            if (self._ready())
                throw Error("can't make ObserveMultiplex ready twice!");
            self._readyFuture.resolve();
            self._readyFuture.isResolved = true;
        });
    }

    // If trying to execute the query results in an error, call this. This is
    // intended for permanent errors, not transient network errors that could be
    // fixed. It should only be called before ready(), because if you called ready
    // that meant that you managed to run the query once. It will stop this
    // ObserveMultiplex and cause addHandleAndSendInitialAdds calls (and thus
    // observeChanges calls) to throw the error.
    queryError(err: Error) {
        var self = this;
        self._queue.runTask(async () => {
            if (self._ready())
                throw Error("can't claim query has an error after it worked!");
            self._stop({ fromQueryError: true });
            self._readyFuture.reject(err);
        });
    }

    // Calls "cb" once the effects of all "ready", "addHandleAndSendInitialAdds"
    // and observe callbacks which came before this call have been propagated to
    // all handles. "ready" must have already been called on this multiplexer.
    onFlush(cb) {
        var self = this;
        self._queue.queueTask(async () => {
            if (!self._ready())
                throw Error("only call onFlush on a multiplexer that will be ready");
            cb();
        });
    }
    callbackNames() {
        var self = this;
        if (self._ordered)
            return ["addedBefore", "changed", "movedBefore", "removed"];
        else
            return ["added", "changed", "removed"];
    }
    _ready() {
        return this._readyFuture.isResolved;
    }
    _applyCallback(callbackName: string, args) {
        var self = this;
        self._queue.queueTask(async () => {
            // If we stopped in the meantime, do nothing.
            if (!self._handles)
                return;

            // First, apply the change to the cache.
            self._cache.applyChange[callbackName].apply(null, args);

            // If we haven't finished the initial adds, then we should only be getting
            // adds.
            if (!self._ready() &&
                (callbackName !== 'added' && callbackName !== 'addedBefore')) {
                throw new Error("Got " + callbackName + " during initial adds");
            }

            // Now multiplex the callbacks out to all observe handles. It's OK if
            // these calls yield; since we're inside a task, no other use of our queue
            // can continue until these are done. (But we do have to be careful to not
            // use a handle that got removed, because removeHandle does not use the
            // queue; thus, we iterate over an array of keys that we control.)
            for (const handleId of Object.keys(self._handles)) {
                var handle = self._handles && self._handles[handleId];
                if (!handle)
                    return;
                var callback = handle['_' + callbackName];
                // clone arguments so that callbacks can mutate their arguments
                callback && callback.apply(null,
                    handle.nonMutatingCallbacks ? args : clone(args));
            }
        });
    }

    // Sends initial adds to a handle. It should only be called from within a task
    // (the task that is processing the addHandleAndSendInitialAdds call). It
    // synchronously invokes the handle's added or addedBefore; there's no need to
    // flush the queue afterwards to ensure that the callbacks get out.
    _sendAdds(handle: ObserveHandle) {
        var self = this;
        //if (self._queue.safeToRunTask())
        //    throw Error("_sendAdds may only be called from within a task!");
        var add = self._ordered ? handle._addedBefore : handle._added;
        if (!add)
            return;
        // note: docs may be an _IdMap or an OrderedDict
        self._cache.docs.forEach(function (doc, id) {
            if (!self._handles.hasOwnProperty(handle._id))
                throw Error("handle got removed before sending initial adds!");
            const { _id, ...fields } = handle.nonMutatingCallbacks ? doc : clone(doc);
            if (self._ordered)
                add(id, fields, null); // we're going in order, so add at end
            else
                add(id, fields);
        });
    }
}


let nextObserveHandleId = 1;

// When the callbacks do not mutate the arguments, we can skip a lot of data clones
export class ObserveHandle {
    
    public _id: number;
    public _addedBefore: Function;
    public _movedBefore: Function;
    public _added: Function;
    public _changed: Function;
    public _removed: Function;
    
    private _stopped: boolean;

    constructor(private _multiplexer: ObserveMultiplexer, callbacks: Record<string, Function>, public nonMutatingCallbacks = false) {
        var self = this;
        // The end user is only supposed to call stop().  The other fields are
        // accessible to the multiplexer, though.
        for (const name of _multiplexer.callbackNames()) {
            if (callbacks[name]) {
                self['_' + name] = callbacks[name];
            } else if (name === "addedBefore" && callbacks.added) {
                // Special case: if you specify "added" and "movedBefore", you get an
                // ordered observe where for some reason you don't get ordering data on
                // the adds.  I dunno, we wrote tests for it, there must have been a
                // reason.
                self._addedBefore = function (id: string, fields: Record<string, any>, before) {
                    callbacks.added(id, fields);
                };
            }
        }
        self._stopped = false;
        self._id = nextObserveHandleId++;
    }

    stop() {
        var self = this;
        if (self._stopped)
            return;
        self._stopped = true;
        self._multiplexer.removeHandle(self._id);
    }
}