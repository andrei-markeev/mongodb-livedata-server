// Ctor for a sub handle: the input to each publish function

import { clone } from "../ejson/ejson";
import { LiveCursor } from "../mongo/live_cursor";
import { Random } from "../random/main";
import { AsyncFunction } from "../types";
import { DDP, maybeAuditArgumentChecks } from "./livedata_server";
import { DDPSession, SessionConnectionHandle } from "./session";

export type SubscriptionHandle = `N${string}` | `U${string}`;
export type SubscriptionCallbacks = Pick<Subscription, "added" | "changed" | "removed">;

// Instance name is this because it's usually referred to as this inside a
// publish
/**
 * @summary The server's side of a subscription
 * @class Subscription
 * @instanceName this
 * @showInstanceName true
 */
export class Subscription {
    public connection: SessionConnectionHandle;
    private _subscriptionHandle: SubscriptionHandle;

    // Has _deactivate been called?
    private _deactivated = false;

    // Stop callbacks to g/c this sub.  called w/ zero arguments.
    private _stopCallbacks: (() => void)[] = [];

    // The set of (collection, documentid) that this subscription has
    // an opinion about.
    private _documents = new Map<string, Set<string>>();

    // Remember if we are ready.
    private _ready = false;

    public userId: string | null;

    // For now, the id filter is going to default to
    // the to/from DDP methods on MongoID, to
    // specifically deal with mongo/minimongo ObjectIds.

    // Later, you will be able to make this be "raw"
    // if you want to publish a collection that you know
    // just has strings for keys and no funny business, to
    // a DDP consumer that isn't minimongo.
    private _idFilter = {
        idStringify: id => id/*MongoID.idStringify*/,
        idParse: id => id/*MongoID.idParse*/
    }

    constructor (
        public _session: DDPSession,
        private _handler: (...args: any[]) => any | AsyncFunction,
        private _subscriptionId: string,
        private _params: any[] = [],
        private _name?: string)
    {

        /**
         * @summary Access inside the publish function. The incoming [connection](#meteor_onconnection) for this subscription.
         * @locus Server
         * @name  connection
         * @memberOf Subscription
         * @instance
         */
        this.connection = _session.connectionHandle; // public API object

        // Only named subscriptions have IDs, but we need some sort of string
        // internally to keep track of all subscriptions inside
        // SessionDocumentViews. We use this subscriptionHandle for that.
        if (this._subscriptionId) {
            this._subscriptionHandle = `N${this._subscriptionId}`;
        } else {
            this._subscriptionHandle = `U${Random.id()}`;
        }


        // Part of the public API: the user of this sub.

        /**
         * @summary Access inside the publish function. The id of the logged-in user, or `null` if no user is logged in.
         * @locus Server
         * @memberOf Subscription
         * @name  userId
         * @instance
         */
        this.userId = this._session.userId;

    };

    async _runHandler() {
        // XXX should we unblock() here? Either before running the publish
        // function, or before running _publishCursor.
        //
        // Right now, each publish function blocks all future publishes and
        // methods waiting on data from Mongo (or whatever else the function
        // blocks on). This probably slows page load in common cases.

        let resultOrThenable = null;
        const oldInvocation = DDP._CurrentPublicationInvocation;
        try {
            DDP._CurrentPublicationInvocation = this;
            resultOrThenable = maybeAuditArgumentChecks(
                this._handler,
                this,
                clone(this._params),
                // It's OK that this would look weird for universal subscriptions,
                // because they have no arguments so there can never be an
                // audit-argument-checks failure.
                "publisher '" + this._name + "'"
            )
        } catch (e) {
            this.error(e);
            return;
        } finally {
            DDP._CurrentPublicationInvocation = oldInvocation;
        }

        // Did the handler call this.error or this.stop?
        if (this._isDeactivated()) return;

        // Both conventional and async publish handler functions are supported.
        // If an object is returned with a then() function, it is either a promise
        // or thenable and will be resolved asynchronously.
        const isThenable =
            resultOrThenable && typeof resultOrThenable.then === 'function';
        if (isThenable) {
            let result;
            try {
                result = await resultOrThenable;
            } catch(e) {
                this.error(e);
            }
            await this._publishHandlerResult(result);
        } else {
            await this._publishHandlerResult(resultOrThenable);
        }
    }

    async _publishHandlerResult(res) {
        // SPECIAL CASE: Instead of writing their own callbacks that invoke
        // this.added/changed/ready/etc, the user can just return a collection
        // cursor or array of cursors from the publish function; we call their
        // _publishCursor method which starts observing the cursor and publishes the
        // results. Note that _publishCursor does NOT call ready().
        //
        // XXX This uses an undocumented interface which only the Mongo cursor
        // interface publishes. Should we make this interface public and encourage
        // users to implement it themselves? Arguably, it's unnecessary; users can
        // already write their own functions like
        //   var publishMyReactiveThingy = function (name, handler) {
        //     Meteor.publish(name, function () {
        //       var reactiveThingy = handler();
        //       reactiveThingy.publishMe();
        //     });
        //   };

        var self = this;
        var isCursor = function (c: any): c is LiveCursor<any> {
            return c && c._publishCursor;
        };
        if (isCursor(res)) {
            try {
                await res._publishCursor(self);
            } catch (e) {
                self.error(e);
                return;
            }
            // _publishCursor only returns after the initial added callbacks have run.
            // mark subscription as ready.
            self.ready();
        } else if (Array.isArray(res)) {
            // Check all the elements are cursors
            if (!res.every(isCursor)) {
                self.error(new Error("Publish function returned an array of non-Cursors"));
                return;
            }
            // Find duplicate collection names
            // XXX we should support overlapping cursors, but that would require the
            // merge box to allow overlap within a subscription
            var collectionNames = {};
            for (var i = 0; i < res.length; ++i) {
                var collectionName = res[i].cursorDescription.collectionName;
                if (collectionNames.hasOwnProperty(collectionName)) {
                    self.error(new Error(
                        "Publish function returned multiple cursors for collection " +
                        collectionName));
                    return;
                }
                collectionNames[collectionName] = true;
            };

            try {
                for (const cur of res) {
                    await cur._publishCursor(self);
                }
            } catch (e) {
                self.error(e);
                return;
            }
            self.ready();
        } else if (res) {
            // Truthy values other than cursors or arrays are probably a
            // user mistake (possible returning a Mongo document via, say,
            // `coll.findOne()`).
            self.error(new Error("Publish function can only return a Cursor or "
                + "an array of Cursors"));
        }
    }

    // This calls all stop callbacks and prevents the handler from updating any
    // SessionCollectionViews further. It's used when the user unsubscribes or
    // disconnects, as well as during setUserId re-runs. It does *NOT* send
    // removed messages for the published objects; if that is necessary, call
    // _removeAllDocuments first.
    _deactivate() {
        var self = this;
        if (self._deactivated)
            return;
        self._deactivated = true;
        self._callStopCallbacks();
        /*Package['facts-base'] && Package['facts-base'].Facts.incrementServerFact(
            "livedata", "subscriptions", -1);*/
    }

    _callStopCallbacks() {
        var self = this;
        // Tell listeners, so they can clean up
        var callbacks = self._stopCallbacks;
        self._stopCallbacks = [];
        for (const callback of callbacks) {
            callback();
        }
    }

    // Send remove messages for every document.
    _removeAllDocuments() {
        var self = this;
        self._documents.forEach(function (collectionDocs, collectionName) {
            collectionDocs.forEach(function (strId) {
                self.removed(collectionName, self._idFilter.idParse(strId));
            });
        });
    }

    // Returns a new Subscription for the same session with the same
    // initial creation parameters. This isn't a clone: it doesn't have
    // the same _documents cache, stopped state or callbacks; may have a
    // different _subscriptionHandle, and gets its userId from the
    // session, not from this object.
    _recreate() {
        var self = this;
        return new Subscription(
            self._session, self._handler, self._subscriptionId, self._params,
            self._name);
    }

    /**
     * @summary Call inside the publish function.  Stops this client's subscription, triggering a call on the client to the `onStop` callback passed to [`Meteor.subscribe`](#meteor_subscribe), if any. If `error` is not a [`Meteor.Error`](#meteor_error), it will be [sanitized](#meteor_error).
     * @locus Server
     * @param {Error} error The error to pass to the client.
     * @instance
     * @memberOf Subscription
     */
    error(error: Error) {
        var self = this;
        if (self._isDeactivated())
            return;
        self._session._stopSubscription(self._subscriptionId, error);
    }

    // Note that while our DDP client will notice that you've called stop() on the
    // server (and clean up its _subscriptions table) we don't actually provide a
    // mechanism for an app to notice this (the subscribe onError callback only
    // triggers if there is an error).

    /**
     * @summary Call inside the publish function.  Stops this client's subscription and invokes the client's `onStop` callback with no error.
     * @locus Server
     * @instance
     * @memberOf Subscription
     */
    stop() {
        var self = this;
        if (self._isDeactivated())
            return;
        self._session._stopSubscription(self._subscriptionId);
    }

    /**
     * @summary Call inside the publish function.  Registers a callback function to run when the subscription is stopped.
     * @locus Server
     * @memberOf Subscription
     * @instance
     * @param {Function} func The callback function
     */
    onStop(func: () => void) {
        var self = this;
        if (self._isDeactivated())
            func();
        else
            self._stopCallbacks.push(func);
    }

    // This returns true if the sub has been deactivated, *OR* if the session was
    // destroyed but the deferred call to _deactivateAllSubscriptions hasn't
    // happened yet.
    _isDeactivated() {
        var self = this;
        return self._deactivated || self._session.inQueue === null;
    }

    /**
     * @summary Call inside the publish function.  Informs the subscriber that a document has been added to the record set.
     * @locus Server
     * @memberOf Subscription
     * @instance
     * @param {String} collection The name of the collection that contains the new document.
     * @param {String} id The new document's ID.
     * @param {Object} fields The fields in the new document.  If `_id` is present it is ignored.
     */
    added(collectionName: string, id: string, fields: Record<string, any>) {
        if (this._isDeactivated())
            return;
        id = this._idFilter.idStringify(id);

        if (this._session.server.getPublicationStrategy(collectionName).doAccountingForCollection) {
            let ids = this._documents.get(collectionName);
            if (ids == null) {
                ids = new Set();
                this._documents.set(collectionName, ids);
            }
            ids.add(id);
        }

        this._session.added(this._subscriptionHandle, collectionName, id, fields);
    }

    /**
     * @summary Call inside the publish function.  Informs the subscriber that a document in the record set has been modified.
     * @locus Server
     * @memberOf Subscription
     * @instance
     * @param {String} collection The name of the collection that contains the changed document.
     * @param {String} id The changed document's ID.
     * @param {Object} fields The fields in the document that have changed, together with their new values.  If a field is not present in `fields` it was left unchanged; if it is present in `fields` and has a value of `undefined` it was removed from the document.  If `_id` is present it is ignored.
     */
    changed(collectionName: string, id: string, fields: Record<string, any>) {
        if (this._isDeactivated())
            return;
        id = this._idFilter.idStringify(id);
        this._session.changed(this._subscriptionHandle, collectionName, id, fields);
    }

    /**
     * @summary Call inside the publish function.  Informs the subscriber that a document has been removed from the record set.
     * @locus Server
     * @memberOf Subscription
     * @instance
     * @param {String} collection The name of the collection that the document has been removed from.
     * @param {String} id The ID of the document that has been removed.
     */
    removed(collectionName: string, id: string) {
        if (this._isDeactivated())
            return;
        id = this._idFilter.idStringify(id);

        if (this._session.server.getPublicationStrategy(collectionName).doAccountingForCollection) {
            // We don't bother to delete sets of things in a collection if the
            // collection is empty.  It could break _removeAllDocuments.
            this._documents.get(collectionName).delete(id);
        }

        this._session.removed(this._subscriptionHandle, collectionName, id);
    }

    /**
     * @summary Call inside the publish function.  Informs the subscriber that an initial, complete snapshot of the record set has been sent.  This will trigger a call on the client to the `onReady` callback passed to  [`Meteor.subscribe`](#meteor_subscribe), if any.
     * @locus Server
     * @memberOf Subscription
     * @instance
     */
    ready() {
        var self = this;
        if (self._isDeactivated())
            return;
        if (!self._subscriptionId)
            return;  // Unnecessary but ignored for universal sub
        if (!self._ready) {
            self._session.sendReady([self._subscriptionId]);
            self._ready = true;
        }
    }

}
