import { _InvalidationCrossbar } from "../ddp/crossbar";
import { _WriteFence } from "../ddp/writefence";
import { LiveMongoConnection } from "./live_connection";
import { ObserveMultiplexer } from "./observe_multiplexer";
import { CursorDescription } from "./live_cursor";
import { DiffSequence } from "../diff-sequence/diff";
import { _SynchronousQueue } from "./synchronous-queue";
import { listenAll } from "./observe_driver_utils";

var POLLING_THROTTLE_MS = +process.env.METEOR_POLLING_THROTTLE_MS || 50;
var POLLING_INTERVAL_MS = +process.env.METEOR_POLLING_INTERVAL_MS || 10 * 1000;

interface PollingObserveDriverOptions<TOrdered extends boolean> {
    cursorDescription: CursorDescription<any>;
    mongoHandle: LiveMongoConnection;
    ordered: TOrdered;
    multiplexer: ObserveMultiplexer;
    stopCallbacks?: (() => void)[];
    matcher?: any;
    sorter?: any;
}

type ResultsType<TOrdered> = TOrdered extends true ? any[] : Map<string, any>;

export class PollingObserveDriver<TOrdered extends boolean> {
    private _cursorDescription: CursorDescription<any>;
    private _mongoHandle: LiveMongoConnection;
    private _ordered: boolean;
    private _multiplexer: ObserveMultiplexer;
    private _stopCallbacks: (() => void)[];
    private _stopped: boolean;

    private _results: ResultsType<TOrdered> | null;
    private _pollsScheduledButNotStarted: number;
    private _pendingWrites: { committed: () => void }[];
    private _ensurePollIsScheduled: Function;
    private _taskQueue: _SynchronousQueue;

    constructor(options: PollingObserveDriverOptions<TOrdered>) {
        var self = this;

        self._cursorDescription = options.cursorDescription;
        self._mongoHandle = options.mongoHandle;
        self._ordered = options.ordered;
        self._multiplexer = options.multiplexer;
        self._stopCallbacks = [];
        self._stopped = false;

        // previous results snapshot.  on each poll cycle, diffs against
        // results drives the callbacks.
        self._results = null;

        // The number of _pollMongo calls that have been added to self._taskQueue but
        // have not started running. Used to make sure we never schedule more than one
        // _pollMongo (other than possibly the one that is currently running). It's
        // also used by _suspendPolling to pretend there's a poll scheduled. Usually,
        // it's either 0 (for "no polls scheduled other than maybe one currently
        // running") or 1 (for "a poll scheduled that isn't running yet"), but it can
        // also be 2 if incremented by _suspendPolling.
        self._pollsScheduledButNotStarted = 0;
        self._pendingWrites = []; // people to notify when polling completes

        // Make sure to create a separately throttled function for each
        // PollingObserveDriver object.
        self._ensurePollIsScheduled = throttle(
            self._unthrottledEnsurePollIsScheduled,
            self._cursorDescription.options.pollingThrottleMs || POLLING_THROTTLE_MS /* ms */
        );

        // XXX figure out if we still need a queue
        self._taskQueue = new _SynchronousQueue();

        var listenersHandle = listenAll(
            self._cursorDescription, function (notification) {
                // When someone does a transaction that might affect us, schedule a poll
                // of the database. If that transaction happens inside of a write fence,
                // block the fence until we've polled and notified observers.
                var fence = _WriteFence._CurrentWriteFence;
                if (fence)
                    self._pendingWrites.push(fence.beginWrite());
                // Ensure a poll is scheduled... but if we already know that one is,
                // don't hit the throttled _ensurePollIsScheduled function (which might
                // lead to us calling it unnecessarily in <pollingThrottleMs> ms).
                if (self._pollsScheduledButNotStarted === 0)
                    self._ensurePollIsScheduled();
            }
        );
        self._stopCallbacks.push(function () { listenersHandle.stop(); });

        // every once and a while, poll even if we don't think we're dirty, for
        // eventual consistency with database writes from outside the Meteor
        // universe.
        var pollingInterval = self._cursorDescription.options.pollingIntervalMs || POLLING_INTERVAL_MS;
        var intervalHandle = setInterval(() => self._ensurePollIsScheduled(), pollingInterval);
        self._stopCallbacks.push(function () {
            clearInterval(intervalHandle);
        });

        // Make sure we actually poll soon!
        self._unthrottledEnsurePollIsScheduled();

    };

    // This is always called through _.throttle (except once at startup).
    _unthrottledEnsurePollIsScheduled() {
        var self = this;
        if (self._pollsScheduledButNotStarted > 0)
            return;
        ++self._pollsScheduledButNotStarted;
        self._taskQueue.queueTask(async () => await self._pollMongo());
    }

    async _pollMongo() {
        var self = this;
        --self._pollsScheduledButNotStarted;

        if (self._stopped)
            return;

        var first = false;
        var newResults: any;
        var oldResults: any = self._results;
        if (!oldResults) {
            first = true;
            // XXX maybe use OrderedDict instead?
            oldResults = self._ordered ? [] : new Map<string, any>();
        }

        // Save the list of pending writes which this round will commit.
        var writesForCycle = self._pendingWrites;
        self._pendingWrites = [];

        // Get the new query results. (This yields.)
        try {
            const cursor = self._mongoHandle.db.collection(self._cursorDescription.collectionName).find(self._cursorDescription.selector);
            if (!self._ordered) {
                newResults = new Map<string, any>();
                for await (const doc of cursor)
                    newResults.set(doc._id, doc);
            } else
                newResults = await cursor.toArray();
        } catch (e) {
            if (first && typeof (e.code) === 'number') {
                // This is an error document sent to us by mongod, not a connection
                // error generated by the client. And we've never seen this query work
                // successfully. Probably it's a bad selector or something, so we should
                // NOT retry. Instead, we should halt the observe (which ends up calling
                // `stop` on us).
                self._multiplexer.queryError(
                    new Error(
                        "Exception while polling query " +
                        JSON.stringify(self._cursorDescription) + ": " + e.message));
                return;
            }

            // getRawObjects can throw if we're having trouble talking to the
            // database.  That's fine --- we will repoll later anyway. But we should
            // make sure not to lose track of this cycle's writes.
            // (It also can throw if there's just something invalid about this query;
            // unfortunately the ObserveDriver API doesn't provide a good way to
            // "cancel" the observe from the inside in this case.
            Array.prototype.push.apply(self._pendingWrites, writesForCycle);
            console.warn("Exception while polling query " + JSON.stringify(self._cursorDescription), e);
            return;
        }

        // Run diffs.
        // This will trigger the callbacks via the multiplexer
        if (!self._stopped) {
            DiffSequence.diffQueryChanges(self._ordered, oldResults, newResults, self._multiplexer);
        }

        // Signals the multiplexer to allow all observeChanges calls that share this
        // multiplexer to return. (This happens asynchronously, via the
        // multiplexer's queue.)
        if (first)
            self._multiplexer.ready();

        // Replace self._results atomically.  (This assignment is what makes `first`
        // stay through on the next cycle, so we've waited until after we've
        // committed to ready-ing the multiplexer.)
        self._results = newResults;

        // Once the ObserveMultiplexer has processed everything we've done in this
        // round, mark all the writes which existed before this call as
        // commmitted. (If new writes have shown up in the meantime, there'll
        // already be another _pollMongo task scheduled.)
        self._multiplexer.onFlush(function () {
            for (const w of writesForCycle) {
                w.committed();
            }
        });
    }

    stop() {
        var self = this;
        self._stopped = true;
        for (const c of self._stopCallbacks) {
            c();
        }
        // Release any write fences that are waiting on us.
        for (const w of self._pendingWrites) {
            w.committed();
        }
    }
}

function throttle(func: Function, wait: number, options?) {
    var timeout, context, args, result;
    var previous = 0;
    if (!options) options = {};

    var later = function () {
        previous = options.leading === false ? 0 : Date.now();
        timeout = null;
        result = func.apply(context, args);
        if (!timeout) context = args = null;
    };

    var throttled = function () {
        var _now = Date.now();
        if (!previous && options.leading === false) previous = _now;
        var remaining = wait - (_now - previous);
        context = this;
        args = arguments;
        if (remaining <= 0 || remaining > wait) {
            if (timeout) {
                clearTimeout(timeout);
                timeout = null;
            }
            previous = _now;
            result = func.apply(context, args);
            if (!timeout) context = args = null;
        } else if (!timeout && options.trailing !== false) {
            timeout = setTimeout(later, remaining);
        }
        return result;
    };

    (throttled as any).cancel = function () {
        clearTimeout(timeout);
        previous = 0;
        timeout = context = args = null;
    };

    return throttled;
}
