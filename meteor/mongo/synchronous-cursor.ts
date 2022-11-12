import MongoDB from "mongodb";
import { equals } from "../ejson/ejson";
import { OPLOG_COLLECTION } from "./oplog_tailing";
import { CursorDescription } from "./live_cursor";

export function _createSynchronousCursor(db: MongoDB.Db, cursorDescription: CursorDescription<any>, options?) {
    const { useTransform } = options || {};

    var collection = db.collection(cursorDescription.collectionName);
    var cursorOptions = cursorDescription.options;
    var mongoOptions = {
        sort: cursorOptions.sort,
        limit: cursorOptions.limit,
        skip: cursorOptions.skip,
        projection: cursorOptions.projection,
        readPreference: cursorOptions.readPreference,
        numberOfRetries: undefined
    };

    // Do we want a tailable cursor (which only works on capped collections)?
    if (cursorOptions.tailable) {
        mongoOptions.numberOfRetries = -1;
    }

    var dbCursor = collection.find(cursorDescription.selector, mongoOptions);

    // Do we want a tailable cursor (which only works on capped collections)?
    if (cursorOptions.tailable) {
        // We want a tailable cursor...
        dbCursor.addCursorFlag("tailable", true)
        // ... and for the server to wait a bit if any getMore has no data (rather
        // than making us put the relevant sleeps in the client)...
        dbCursor.addCursorFlag("awaitData", true)

        // And if this is on the oplog collection and the cursor specifies a 'ts',
        // then set the undocumented oplog replay flag, which does a special scan to
        // find the first document (instead of creating an index on ts). This is a
        // very hard-coded Mongo flag which only works on the oplog collection and
        // only works with the ts field.
        if (cursorDescription.collectionName === OPLOG_COLLECTION &&
            cursorDescription.selector.ts) {
            dbCursor.addCursorFlag("oplogReplay", true)
        }
    }

    if (typeof cursorOptions.maxTimeMs !== 'undefined') {
        dbCursor = dbCursor.maxTimeMS(cursorOptions.maxTimeMs);
    }
    if (typeof cursorOptions.hint !== 'undefined') {
        dbCursor = dbCursor.hint(cursorOptions.hint);
    }

    return new SynchronousCursor(dbCursor, cursorDescription, { useTransform });
};

export class SynchronousCursor {

    private _transform: any;
    private _visitedIds: Set<string>;

    constructor(
        private _dbCursor: MongoDB.FindCursor,
        private _cursorDescription: CursorDescription<any>,
        options: { useTransform?: boolean }
    ) {
        var self = this;

        if (options.useTransform && _cursorDescription.options.transform) {
            self._transform = wrapTransform(_cursorDescription.options.transform);
        } else {
            self._transform = null;
        }

        self._visitedIds = new Set();
    }

    // Returns a Promise for the next object from the cursor, skipping those whose
    // IDs we've already seen and replacing Mongo atoms with Meteor atoms.
    async _nextObjectPromise() {
        var self = this;

        while (true) {
            var doc = await this._dbCursor.next();

            if (!doc) return null;

            if (!self._cursorDescription.options.tailable && doc.hasOwnProperty('_id')) {
                // Did Mongo give us duplicate documents in the same cursor? If so,
                // ignore this one. (Do this before the transform, since transform might
                // return some unrelated value.) We don't do this for tailable cursors,
                // because we want to maintain O(1) memory usage. And if there isn't _id
                // for some reason (maybe it's the oplog), then we don't do this either.
                // (Be careful to do this for falsey but existing _id, though.)
                if (self._visitedIds.has(doc._id)) continue;
                self._visitedIds.add(doc._id);
            }

            if (self._transform)
                doc = self._transform(doc);

            return doc;
        }
    }

    // Returns a promise which is resolved with the next object (like with
    // _nextObjectPromise) or rejected if the cursor doesn't return within
    // timeoutMS ms.
    async _nextObjectPromiseWithTimeout(timeoutMS: number) {
        if (!timeoutMS) {
            return this._nextObjectPromise();
        }
        const nextObjectPromise = this._nextObjectPromise();
        const timeoutErr = new Error('Client-side timeout waiting for next object');
        const timeoutPromise = new Promise<void>((_resolve, reject) => {
            setTimeout(() => {
                reject(timeoutErr);
            }, timeoutMS);
        });
        return Promise.race([nextObjectPromise, timeoutPromise])
            .catch((err) => {
                if (err === timeoutErr) {
                    this._dbCursor.close();
                }
                throw err;
            });
    }

    close() {
        this._dbCursor.close();
    }

    async forEach(callback: (doc: any, index: number, cursor: SynchronousCursor) => void, thisArg?) {
        var self = this;

        // Get back to the beginning.
        self._rewind();

        // We implement the loop ourself instead of using self._dbCursor.each,
        // because "each" will call its callback outside of a fiber which makes it
        // much more complex to make this function synchronous.
        var index = 0;
        while (true) {
            var doc = await self._nextObjectPromise();
            if (!doc) return;
            callback.call(thisArg, doc, index++, self);
        }
    }

    _rewind() {
        var self = this;

        // known to be synchronous
        self._dbCursor.rewind();

        self._visitedIds = new Set();
    }
}

// Wrap a transform function to return objects that have the _id field
// of the untransformed document. This ensures that subsystems such as
// the observe-sequence package that call `observe` can keep track of
// the documents identities.
//
// - Require that it returns objects
// - If the return value has an _id field, verify that it matches the
//   original _id field
// - If the return value doesn't have an _id field, add it back.
function wrapTransform(transform: Function & { __wrappedTransform__?: boolean }) {
    if (!transform)
        return null;

    // No need to doubly-wrap transforms.
    if (transform.__wrappedTransform__)
        return transform;

    const wrapped = doc => {
        if (!doc.hasOwnProperty('_id')) {
            // XXX do we ever have a transform on the oplog's collection? because that
            // collection has no _id.
            throw new Error('can only transform documents with _id');
        }

        const id = doc._id;

        const transformed = transform(doc);

        if (transformed.hasOwnProperty('_id')) {
            if (!equals(transformed._id, id)) {
                throw new Error('transformed document can\'t have different _id');
            }
        } else {
            transformed._id = id;
        }

        return transformed;
    };

    wrapped.__wrappedTransform__ = true;

    return wrapped;
};


/*
    forEach(callback, thisArg) {
        var self = this;

        // Get back to the beginning.
        self._rewind();

        // We implement the loop ourself instead of using self._dbCursor.each,
        // because "each" will call its callback outside of a fiber which makes it
        // much more complex to make this function synchronous.
        var index = 0;
        while (true) {
            var doc = self._nextObject();
            if (!doc) return;
            callback.call(thisArg, doc, index++, self._selfForIteration);
        }
    }

    // XXX Allow overlapping callback executions if callback yields.
    map(callback, thisArg) {
        var self = this;
        var res = [];
        self.forEach(function (doc, index) {
            res.push(callback.call(thisArg, doc, index, self._selfForIteration));
        });
        return res;
    },

    _rewind() {
        var self = this;

        // known to be synchronous
        self._dbCursor.rewind();

        self._visitedIds = new LocalCollection._IdMap;
    }

    // Mostly usable for tailable cursors.
    close() {
        var self = this;

        self._dbCursor.close();
    }

    fetch() {
        var self = this;
        return self.map(_.identity);
    }

    // This method is NOT wrapped in Cursor.
    getRawObjects(ordered) {
        var self = this;
        if (ordered) {
            return self.fetch();
        } else {
            var results = new LocalCollection._IdMap;
            self.forEach(function (doc) {
                results.set(doc._id, doc);
            });
            return results;
        }
    }

    [Symbol.iterator]() {
        var self = this;

        // Get back to the beginning.
        self._rewind();

        return {
            next() {
                const doc = self._nextObject();
                return doc ? {
                    value: doc
                } : {
                    done: true
                };
            }
        };
    };

    [Symbol.asyncIterator]() {
        const syncResult = this[Symbol.iterator]();
        return {
            async next() {
                return Promise.resolve(syncResult.next());
            }
        };
    }
*/