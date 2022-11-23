import { DiffSequence } from "../diff-sequence/diff";
import { equals } from "../ejson/ejson";
import { IdMap } from "../id-map/id_map";
import { SessionDocumentView } from "./session-document-view";
import { SubscriptionCallbacks, SubscriptionHandle } from "./subscription";

/**
 * Represents a client's view of a single collection
 * @param {String} collectionName Name of the collection it represents
 * @param {Object.<String, Function>} sessionCallbacks The callbacks for added, changed, removed
 * @class SessionCollectionView
 */
export class SessionCollectionView {

    private documents = new IdMap();
    constructor (private collectionName: string, private callbacks: SubscriptionCallbacks) { }

    isEmpty() {
        return this.documents.empty();
    }

    diff(previous: SessionCollectionView) {
        DiffSequence.diffMaps(previous.documents, this.documents, {
            both: this.diffDocument.bind(this),

            rightOnly: (id, nowDV) => {
                this.callbacks.added(this.collectionName, id, nowDV.getFields());
            },

            leftOnly: (id, prevDV) => {
                this.callbacks.removed(this.collectionName, id);
            }
        });
    }

    diffDocument(id: string, prevDV: SessionDocumentView, nowDV: SessionDocumentView) {
        const fields = {};
        DiffSequence.diffObjects(prevDV.getFields(), nowDV.getFields(), {
            both: (key: string, prev: any, now: any) => {
                if (!equals(prev, now))
                    fields[key] = now;
            },
            rightOnly: (key: string, now: any) => {
                fields[key] = now;
            },
            leftOnly: (key: string, prev: any) => {
                fields[key] = undefined;
            }
        });
        this.callbacks.changed(this.collectionName, id, fields);
    }

    added(subscriptionHandle: SubscriptionHandle, id: string, fields: Record<string, any>) {
        var self = this;
        var docView = self.documents.get(id);
        var added = false;
        if (!docView) {
            added = true;
            docView = new SessionDocumentView();
            self.documents.set(id, docView);
        }
        docView.existsIn.add(subscriptionHandle);
        var changeCollector = {};
        for (const [key, value] of Object.entries(fields)) {
            docView.changeField(subscriptionHandle, key, value, changeCollector, true);
        }
        if (added)
            self.callbacks.added(self.collectionName, id, changeCollector);
        else
            self.callbacks.changed(self.collectionName, id, changeCollector);
    }

    changed(subscriptionHandle: SubscriptionHandle, id: string, changed: Record<string, any>) {
        var self = this;
        var changedResult = {};
        var docView = self.documents.get(id);
        if (!docView)
            throw new Error("Could not find element with id " + id + " to change");
        for (const [key, value] of Object.entries(changed)) {
            if (value === undefined)
                docView.clearField(subscriptionHandle, key, changedResult);
            else
                docView.changeField(subscriptionHandle, key, value, changedResult);
        }
        self.callbacks.changed(self.collectionName, id, changedResult);
    }

    removed(subscriptionHandle: SubscriptionHandle, id: string) {
        var self = this;
        var docView = self.documents.get(id);
        if (!docView) {
            var err = new Error("Removed nonexistent document " + id);
            throw err;
        }
        docView.existsIn.delete(subscriptionHandle);
        if (docView.existsIn.size === 0) {
            // it is gone from everyone
            self.callbacks.removed(self.collectionName, id);
            self.documents.remove(id);
        } else {
            var changed = {};
            // remove this subscription from every precedence list
            // and record the changes
            docView.dataByKey.forEach((precedenceList, key) => {
                docView.clearField(subscriptionHandle, key, changed);
            });

            self.callbacks.changed(self.collectionName, id, changed);
        }
    }
}
