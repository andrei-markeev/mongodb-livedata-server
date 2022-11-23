import { Hook } from "../callback-hook/hook";
import { MongoClient, Db } from "mongodb";
import { CursorDescription } from "./live_cursor";
import { stringify } from "../ejson/ejson";
import { _createSynchronousCursor } from "./synchronous-cursor";
import { OplogHandle } from "./oplog_tailing";
import { DocFetcher } from "./doc_fetcher";
import { ObserveCallbacks, ObserveHandle, ObserveMultiplexer } from "./observe_multiplexer";
import { PollingObserveDriver } from "./polling_observe_driver";
import { OplogObserveDriver } from "./oplog-observe-driver";
import { MinimongoMatcher } from "./minimongo_matcher";
import MinimongoSorter from "./minimongo_sorter";

export class LiveMongoConnection {

    private client: MongoClient;
    public db: Db;

    public _oplogHandle: OplogHandle = null;
    public _docFetcher: DocFetcher = null;

    private _observeMultiplexers: Record<string, any> = {};
    private _onFailoverHook: Hook = new Hook();

    constructor(url: string, options?) {
        options = options || {};

        var mongoOptions = {
            ignoreUndefined: true,
            maxPoolSize: undefined
        };

        // Internally the oplog connections specify their own maxPoolSize
        // which we don't want to overwrite with any user defined value
        if (options.hasOwnProperty("maxPoolSize")) {
            // If we just set this for "server", replSet will override it. If we just
            // set it for replSet, it will be ignored if we're not using a replSet.
            mongoOptions.maxPoolSize = options.maxPoolSize;
        }

        this.client = new MongoClient(url, mongoOptions);
        this.db = this.client.db();

        this.client.on('serverDescriptionChanged', event => {
            // When the connection is no longer against the primary node, execute all
            // failover hooks. This is important for the driver as it has to re-pool the
            // query when it happens.
            if (
                event.previousDescription.type !== 'RSPrimary' &&
                event.newDescription.type === 'RSPrimary'
            ) {
                this._onFailoverHook.each(callback => {
                    callback();
                    return true;
                });
            }
        });

        if (options.oplogUrl) {
            this._oplogHandle = new OplogHandle(options.oplogUrl, this.db.databaseName);
            this._docFetcher = new DocFetcher(this.db);
        }
    }

    close() {
        var self = this;

        if (!self.db)
            throw Error("close called before Connection created?");

        // XXX probably untested
        var oplogHandle = self._oplogHandle;
        self._oplogHandle = null;
        if (oplogHandle)
            oplogHandle.stop();

        // Use Future.wrap so that errors get thrown. This happens to
        // work even outside a fiber since the 'close' method is not
        // actually asynchronous.
        self.client.close(true);
    }

    // Tails the cursor described by cursorDescription, most likely on the
    // oplog. Calls docCallback with each document found. Ignores errors and just
    // restarts the tail on error.
    //
    // If timeoutMS is set, then if we don't get a new document every timeoutMS,
    // kill and restart the cursor. This is primarily a workaround for #8598.
    tail<T>(cursorDescription: CursorDescription<T>, docCallback: (doc: T) => void, timeoutMS?: number) {
        if (!cursorDescription.options.tailable)
            throw new Error("Can only tail a tailable cursor");

        var cursor = _createSynchronousCursor(this.db, cursorDescription);

        var stopped = false;
        var lastTS: number;
        const loop = async () => {
            var doc = null;
            while (true) {
                if (stopped)
                    return;
                try {
                    doc = await cursor._nextObjectPromiseWithTimeout(timeoutMS);
                } catch (err) {
                    // There's no good way to figure out if this was actually an error from
                    // Mongo, or just client-side (including our own timeout error). Ah
                    // well. But either way, we need to retry the cursor (unless the failure
                    // was because the observe got stopped).
                    doc = null;
                }
                // Since we awaited a promise above, we need to check again to see if
                // we've been stopped before calling the callback.
                if (stopped)
                    return;
                if (doc) {
                    // If a tailable cursor contains a "ts" field, use it to recreate the
                    // cursor on error. ("ts" is a standard that Mongo uses internally for
                    // the oplog, and there's a special flag that lets you do binary search
                    // on it instead of needing to use an index.)
                    lastTS = doc.ts;
                    docCallback(doc);
                } else {
                    const newSelector = { ...cursorDescription.selector };
                    if (lastTS) {
                        (newSelector as any).ts = { $gt: lastTS };
                    }
                    const newDescription = new CursorDescription(
                        cursorDescription.collectionName,
                        newSelector,
                        cursorDescription.options
                    );
                    cursor = _createSynchronousCursor(this.db, newDescription);
                    // Mongo failover takes many seconds.  Retry in a bit.  (Without this
                    // setTimeout, we peg the CPU at 100% and never notice the actual
                    // failover.
                    setTimeout(loop, 100);
                    break;
                }
            }
        };

        setImmediate(loop);

        return {
            stop: function () {
                stopped = true;
                cursor.close();
            }
        };
    }

    async _observeChanges(cursorDescription: CursorDescription<any>, ordered: boolean, callbacks: ObserveCallbacks, nonMutatingCallbacks: boolean) {
        var self = this;

        if (cursorDescription.options.tailable) {
            return self._observeChangesTailable(cursorDescription, ordered, callbacks);
        }

        // You may not filter out _id when observing changes, because the id is a core
        // part of the observeChanges API.
        const fieldsOptions = cursorDescription.options.projection;
        if (fieldsOptions && (fieldsOptions._id === 0 || fieldsOptions._id === false))
            throw Error("You may not observe a cursor with {fields: {_id: 0}}");

        var observeKey = stringify(Object.assign({ ordered: ordered }, cursorDescription));

        var multiplexer: ObserveMultiplexer, observeDriver: OplogObserveDriver | PollingObserveDriver<any>;
        var firstHandle = false;

        // Find a matching ObserveMultiplexer, or create a new one. This next block is
        // guaranteed to not yield (and it doesn't call anything that can observe a
        // new query), so no other calls to this function can interleave with it.
        //Meteor._noYieldsAllowed(function () {
        if (self._observeMultiplexers.hasOwnProperty(observeKey)) {
            multiplexer = self._observeMultiplexers[observeKey];
        } else {
            firstHandle = true;
            // Create a new ObserveMultiplexer.
            multiplexer = new ObserveMultiplexer({
                ordered: ordered,
                onStop: function () {
                    delete self._observeMultiplexers[observeKey];
                    observeDriver.stop();
                }
            });
            self._observeMultiplexers[observeKey] = multiplexer;
        }
        //});

        var observeHandle = new ObserveHandle(
            multiplexer,
            callbacks,
            nonMutatingCallbacks
        );

        if (firstHandle) {
            let matcher: MinimongoMatcher;
            let sorter: MinimongoSorter;

            // At a bare minimum, using the oplog requires us to have an oplog, to
            // want unordered callbacks, and to not want a callback on the polls
            // that won't happen.
            const basicPrerequisites = self._oplogHandle && !ordered && !callbacks._testOnlyPollCallback;

            let selectorIsCompilable = false;
            // We need to be able to compile the selector. Fall back to polling for
            // some newfangled $selector that minimongo doesn't support yet.
            try {
                matcher = new MinimongoMatcher(cursorDescription.selector);
                selectorIsCompilable = true;
            } catch (e) {
            }

            const supportedByOplog = OplogObserveDriver.cursorSupported(cursorDescription, matcher);

            let cursorIsSortable = false;
            // And we need to be able to compile the sort, if any.  eg, can't be
            // {$natural: 1}.
            if (!cursorDescription.options.sort)
                cursorIsSortable = true;
            try {
                sorter = new MinimongoSorter(cursorDescription.options.sort);
                cursorIsSortable = true;
            } catch (e) {
            }

            const canUseOplog = basicPrerequisites && selectorIsCompilable && cursorIsSortable && supportedByOplog;

            var driverClass = canUseOplog ? OplogObserveDriver : PollingObserveDriver;
            observeDriver = new driverClass({
                cursorDescription: cursorDescription,
                mongoHandle: self,
                multiplexer: multiplexer,
                ordered: ordered,
                matcher,  // ignored by polling
                sorter  // ignored by polling
            });
        }

        // Blocks until the initial adds have been sent.
        await multiplexer.addHandleAndSendInitialAdds(observeHandle);

        return observeHandle;
    }

    // observeChanges for tailable cursors on capped collections.
    //
    // Some differences from normal cursors:
    //   - Will never produce anything other than 'added' or 'addedBefore'. If you
    //     do update a document that has already been produced, this will not notice
    //     it.
    //   - If you disconnect and reconnect from Mongo, it will essentially restart
    //     the query, which will lead to duplicate results. This is pretty bad,
    //     but if you include a field called 'ts' which is inserted as
    //     new MongoInternals.MongoTimestamp(0, 0) (which is initialized to the
    //     current Mongo-style timestamp), we'll be able to find the place to
    //     restart properly. (This field is specifically understood by Mongo with an
    //     optimization which allows it to find the right place to start without
    //     an index on ts. It's how the oplog works.)
    //   - No callbacks are triggered synchronously with the call (there's no
    //     differentiation between "initial data" and "later changes"; everything
    //     that matches the query gets sent asynchronously).
    //   - De-duplication is not implemented.
    //   - Does not yet interact with the write fence. Probably, this should work by
    //     ignoring removes (which don't work on capped collections) and updates
    //     (which don't affect tailable cursors), and just keeping track of the ID
    //     of the inserted object, and closing the write fence once you get to that
    //     ID (or timestamp?).  This doesn't work well if the document doesn't match
    //     the query, though.  On the other hand, the write fence can close
    //     immediately if it does not match the query. So if we trust minimongo
    //     enough to accurately evaluate the query against the write fence, we
    //     should be able to do this...  Of course, minimongo doesn't even support
    //     Mongo Timestamps yet.
    _observeChangesTailable(cursorDescription: CursorDescription<any>, ordered: boolean, callbacks: any) {
        var self = this;

        // Tailable cursors only ever call added/addedBefore callbacks, so it's an
        // error if you didn't provide them.
        if ((ordered && !callbacks.addedBefore) ||
            (!ordered && !callbacks.added)) {
            throw new Error("Can't observe an " + (ordered ? "ordered" : "unordered")
                + " tailable cursor without a "
                + (ordered ? "addedBefore" : "added") + " callback");
        }

        return self.tail(cursorDescription, function (doc) {
            var id = doc._id;
            delete doc._id;
            // The ts is an implementation detail. Hide it.
            delete doc.ts;
            if (ordered) {
                callbacks.addedBefore(id, doc, null);
            } else {
                callbacks.added(id, doc);
            }
        });
    }

    _onFailover(callback: Function) {
        return this._onFailoverHook.register(callback);
    }
}