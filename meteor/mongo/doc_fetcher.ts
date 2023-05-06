import * as MongoDB from "mongodb";
import { clone } from "../ejson/ejson";

export class DocFetcher {
    private _callbacksForOp = new Map<Object, Function[]>();
    constructor(private db: MongoDB.Db) {
    }

    // Fetches document "id" from collectionName, returning it or null if not
    // found.
    //
    // If you make multiple calls to fetch() with the same op reference,
    // DocFetcher may assume that they all return the same document. (It does
    // not check to see if collectionName/id match.)
    //
    // You may assume that callback is never called synchronously (and in fact
    // OplogObserveDriver does so).
    async fetch(collectionName: string, id: string, op: Object, callback: Function) {
        const self = this;

        // If there's already an in-progress fetch for this cache key, yield until
        // it's done and return whatever it returns.
        if (self._callbacksForOp.has(op)) {
            self._callbacksForOp.get(op).push(callback);
            return;
        }

        const callbacks = [callback];
        self._callbacksForOp.set(op, callbacks);

        try {
            var doc = await self.db.collection<{ _id: string }>(collectionName).findOne({ _id: id }) || null;
            // Return doc to all relevant callbacks. Note that this array can
            // continue to grow during callback execution.
            while (callbacks.length > 0) {
                // Clone the document so that the various calls to fetch don't return
                // objects that are intertwingled with each other. Clone before
                // popping the future, so that if clone throws, the error gets passed
                // to the next callback.
                callbacks.pop()(null, clone(doc));
            }
        } catch (e) {
            while (callbacks.length > 0) {
                callbacks.pop()(e);
            }
        } finally {
            // XXX consider keeping the doc around for a period of time before
            // removing from the cache
            self._callbacksForOp.delete(op);
        }
    }
}