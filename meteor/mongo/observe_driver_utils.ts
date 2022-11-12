import MongoDB from "mongodb";
import { _InvalidationCrossbar } from "../ddp/crossbar";
import { CursorDescription } from "./live_cursor";

// Listen for the invalidation messages that will trigger us to poll the
// database for changes. If this selector specifies specific IDs, specify them
// here, so that updates to different specific IDs don't cause us to poll.
// listenCallback is the same kind of (notification, complete) callback passed
// to InvalidationCrossbar.listen.

export function listenAll(cursorDescription: CursorDescription<any>, listenCallback: Function) {
    var listeners = [];
    forEachTrigger(cursorDescription, function (trigger) {
        listeners.push(_InvalidationCrossbar.listen(trigger, listenCallback));
    });

    return {
        stop: function () {
            for (const listener of listeners) {
                listener.stop();
            }
        }
    };
}

export function forEachTrigger(cursorDescription: CursorDescription<any>, triggerCallback: Function) {
    var key = { collection: cursorDescription.collectionName };
    var specificIds = _idsMatchedBySelector(cursorDescription.selector);
    if (specificIds) {
        for (const id of specificIds) {
            triggerCallback(Object.assign({ id: id }, key));
        }
        triggerCallback(Object.assign({ dropCollection: true, id: null }, key));
    } else {
        triggerCallback(key);
    }
    // Everyone cares about the database being dropped.
    triggerCallback({ dropDatabase: true });
}

export function _idsMatchedBySelector(selector: MongoDB.Filter<{ _id: string }>) {
    if (!selector) {
        return null;
    }

    // Do we have an _id clause?
    if (selector.hasOwnProperty('_id')) {
        // Is the _id clause just an ID?
        if (typeof selector._id === "string") {
            return [selector._id];
        }

        // Is the _id clause {_id: {$in: ["x", "y", "z"]}}?
        if (selector._id
            && "$in" in selector._id
            && Array.isArray(selector._id.$in)
            && selector._id.$in.length
            && selector._id.$in.every(id => typeof id === "string")) {
            return selector._id.$in;
        }

        return null;
    }

    // If this is a top-level $and, and any of the clauses constrain their
    // documents, then the whole selector is constrained by any one clause's
    // constraint. (Well, by their intersection, but that seems unlikely.)
    if ("$and" in selector && Array.isArray(selector.$and)) {
        for (let i = 0; i < selector.$and.length; ++i) {
            const subIds = _idsMatchedBySelector(selector.$and[i]);

            if (subIds) {
                return subIds;
            }
        }
    }

    return null;
}