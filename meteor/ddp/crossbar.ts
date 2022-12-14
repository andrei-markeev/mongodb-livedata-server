import { clone, equals } from "../ejson/ejson";

// A "crossbar" is a class that provides structured notification registration.
// See _match for the definition of how a notification matches a trigger.
// All notifications and triggers must have a string key named 'collection'.

export class _Crossbar {
    private listenersByCollection: Record<string, any> = {};
    private listenersByCollectionCount: Record<string, number> = {};
    private nextId: number;

    constructor(options) {
        options = options || {};

        this.nextId = 1;
    }

    // msg is a trigger or a notification
    _collectionForMessage(msg: { collection?: any }) {
        if (!msg.hasOwnProperty('collection')) {
            return '';
        } else if (typeof (msg.collection) === 'string') {
            if (msg.collection === '')
                throw Error("Message has empty collection!");
            return msg.collection;
        } else {
            throw Error("Message has non-string collection!");
        }
    }

    // Listen for notification that match 'trigger'. A notification
    // matches if it has the key-value pairs in trigger as a
    // subset. When a notification matches, call 'callback', passing
    // the actual notification.
    //
    // Returns a listen handle, which is an object with a method
    // stop(). Call stop() to stop listening.
    //
    // XXX It should be legal to call fire() from inside a listen()
    // callback?
    listen(trigger, callback) {
        var self = this;
        var id = self.nextId++;

        var collection = self._collectionForMessage(trigger);
        var record = { trigger: clone(trigger), callback: callback };
        if (!self.listenersByCollection.hasOwnProperty(collection)) {
            self.listenersByCollection[collection] = {};
            self.listenersByCollectionCount[collection] = 0;
        }
        self.listenersByCollection[collection][id] = record;
        self.listenersByCollectionCount[collection]++;

        return {
            stop: function () {
                delete self.listenersByCollection[collection][id];
                self.listenersByCollectionCount[collection]--;
                if (self.listenersByCollectionCount[collection] === 0) {
                    delete self.listenersByCollection[collection];
                    delete self.listenersByCollectionCount[collection];
                }
            }
        };
    }

    // Fire the provided 'notification' (an object whose attribute
    // values are all JSON-compatibile) -- inform all matching listeners
    // (registered with listen()).
    //
    // If fire() is called inside a write fence, then each of the
    // listener callbacks will be called inside the write fence as well.
    //
    // The listeners may be invoked in parallel, rather than serially.
    fire(notification: Record<string, any>) {
        var self = this;

        var collection = self._collectionForMessage(notification);

        if (!self.listenersByCollection.hasOwnProperty(collection)) {
            return;
        }

        var listenersForCollection = self.listenersByCollection[collection];
        var callbackIds = [];
        Object.entries<{ trigger: Record<string, any> }>(listenersForCollection).forEach(function ([id, l]) {
            if (self._matches(notification, l.trigger)) {
                callbackIds.push(id);
            }
        });

        // Listener callbacks can yield, so we need to first find all the ones that
        // match in a single iteration over self.listenersByCollection (which can't
        // be mutated during this iteration), and then invoke the matching
        // callbacks, checking before each call to ensure they haven't stopped.
        // Note that we don't have to check that
        // self.listenersByCollection[collection] still === listenersForCollection,
        // because the only way that stops being true is if listenersForCollection
        // first gets reduced down to the empty object (and then never gets
        // increased again).
        for (const id of callbackIds) {
            if (listenersForCollection.hasOwnProperty(id)) {
                listenersForCollection[id].callback(notification);
            }
        }
    }

    // A notification matches a trigger if all keys that exist in both are equal.
    //
    // Examples:
    //  N:{collection: "C"} matches T:{collection: "C"}
    //    (a non-targeted write to a collection matches a
    //     non-targeted query)
    //  N:{collection: "C", id: "X"} matches T:{collection: "C"}
    //    (a targeted write to a collection matches a non-targeted query)
    //  N:{collection: "C"} matches T:{collection: "C", id: "X"}
    //    (a non-targeted write to a collection matches a
    //     targeted query)
    //  N:{collection: "C", id: "X"} matches T:{collection: "C", id: "X"}
    //    (a targeted write to a collection matches a targeted query targeted
    //     at the same document)
    //  N:{collection: "C", id: "X"} does not match T:{collection: "C", id: "Y"}
    //    (a targeted write to a collection does not match a targeted query
    //     targeted at a different document)
    _matches(notification: Record<string, any>, trigger: Record<string, any>) {
        // Most notifications that use the crossbar have a string `collection` and
        // maybe an `id` that is a string or ObjectID. We're already dividing up
        // triggers by collection, but let's fast-track "nope, different ID" (and
        // avoid the overly generic EJSON.equals). This makes a noticeable
        // performance difference; see https://github.com/meteor/meteor/pull/3697
        if (typeof (notification.id) === 'string' && typeof (trigger.id) === 'string' && notification.id !== trigger.id) {
            return false;
        }

        return Object.entries(trigger).every(([key, triggerValue]) => {
            return !notification.hasOwnProperty(key) || equals(triggerValue, notification[key]);
        });
    }

}

// The "invalidation crossbar" is a specific instance used by the DDP server to
// implement write fence notifications. Listener callbacks on this crossbar
// should call beginWrite on the current write fence before they return, if they
// want to delay the write fence from firing (ie, the DDP method-data-updated
// message from being sent).
export const _InvalidationCrossbar = new _Crossbar({
    factName: "invalidation-crossbar-listeners"
});