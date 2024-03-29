import { Subscription } from "../ddp/subscription";
import * as MongoDB from "mongodb";
import { LiveMongoConnection } from "./live_connection";
import { Random } from "../random/main";
import { OrderedDict } from "../ordered-dict/ordered_dict";

interface CustomFindOptions<T> extends MongoDB.FindOptions<MongoDB.WithId<T>> {
    pollingThrottleMs?: number;
    pollingIntervalMs?: number;
    transform?: (doc: T) => T;
    maxTimeMs?: number;
    disableOplog?: boolean;
}

export class CursorDescription<T> {
    public selector: MongoDB.Filter<MongoDB.WithId<T>>;
    public options: CustomFindOptions<MongoDB.WithId<T>>;
    constructor(public collectionName: string, selector: MongoDB.Filter<MongoDB.WithId<T>>, options?: CustomFindOptions<MongoDB.WithId<T>>) {
        var self = this;
        self.collectionName = collectionName;
        self.selector = _rewriteSelector(selector);
        self.options = options || {};
    }
}

export class LiveCursor<T> {
    public cursorDescription: CursorDescription<T>;

    constructor (public mongo: LiveMongoConnection, collectionName: string, selector: MongoDB.Filter<MongoDB.WithId<T>>, options: CustomFindOptions<MongoDB.WithId<T>>) {
        this.cursorDescription = new CursorDescription(collectionName, selector, options);
    }

    async _publishCursor(sub: Subscription) {
        const observeHandle = await this.mongo._observeChanges(
            this.cursorDescription,
            false,
            {
                initialAdds: (docs: Map<string, T> | OrderedDict) => {
                    sub.initialAdds(this.cursorDescription.collectionName, docs);
                },
                added: (id: string, fields: Omit<T, "_id">) => {
                    sub.added(this.cursorDescription.collectionName, id, fields);
                },
                changed: (id: string, fields: Partial<T>) => {
                    sub.changed(this.cursorDescription.collectionName, id, fields);
                },
                removed: (id: string) => {
                    sub.removed(this.cursorDescription.collectionName, id);
                },
            },
            // Publications don't mutate the documents
            // This is tested by the `livedata - publish callbacks clone` test
            true
        );
    
        // We don't call sub.ready() here: it gets called in livedata_server, after
        // possibly calling _publishCursor on multiple returned cursors.
    
        // register stop callback (expects lambda w/ no args).
        sub.onStop(function() {
            observeHandle.stop();
        });
    
        // return the observeHandle in case it needs to be stopped early
        return observeHandle;
    }
}


function _rewriteSelector<T>(selector: MongoDB.Filter<MongoDB.WithId<T>>) {
    if (Array.isArray(selector)) {
      // This is consistent with the Mongo console itself; if we don't do this
      // check passing an empty array ends up selecting all items
      throw new Error("Mongo selector can't be an array.");
    }

    if (!selector || ('_id' in selector && !selector._id)) {
      // can't match anything
      return { _id: Random.id() } as MongoDB.Filter<MongoDB.WithId<T>>;
    }

    return selector;
}