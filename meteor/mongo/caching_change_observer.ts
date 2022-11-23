// _CachingChangeObserver is an object which receives observeChanges callbacks
// and keeps a cache of the current cursor state up to date in this.docs. Users
// of this class should read the docs field but not modify it. You should pass
// the "applyChange" field as the callbacks to the underlying observeChanges
// call. Optionally, you can specify your own observeChanges callbacks which are
// invoked immediately before the docs field is updated; this object is made

import { DiffSequence } from "../diff-sequence/diff";
import { clone } from "../ejson/ejson";
import { IdMap } from "../id-map/id_map";
import { OrderedDict } from "../ordered-dict/ordered_dict";

// available as `this` to those callbacks.
export class _CachingChangeObserver {
    public docs: OrderedDict | IdMap;
    public applyChange: {
        added?: (id: string, fields: any) => void;
        changed?: (id: string, fields: any) => void;
        removed?: (id: string) => void;
        addedBefore?: (id: string, fields: any, before: any) => void;
        movedBefore?: (id: string, before: any) => void;
    };

    private ordered: boolean;

    constructor(options: { callbacks?: any, ordered?: boolean } = {}) {
      const orderedFromCallbacks = (
        options.callbacks &&
        !!(options.callbacks.addedBefore || options.callbacks.movedBefore)
      );
  
      if (options.hasOwnProperty('ordered')) {
        this.ordered = options.ordered;
  
        if (options.callbacks && options.ordered !== orderedFromCallbacks) {
          throw Error('ordered option doesn\'t match callbacks');
        }
      } else if (options.callbacks) {
        this.ordered = orderedFromCallbacks;
      } else {
        throw Error('must provide ordered or callbacks');
      }
  
      const callbacks = options.callbacks || {};
  
      if (this.ordered) {
        this.docs = new OrderedDict();
        this.applyChange = {
          addedBefore: (id, fields, before) => {
            // Take a shallow copy since the top-level properties can be changed
            const doc = { ...fields };
  
            doc._id = id;
  
            if (callbacks.addedBefore) {
              callbacks.addedBefore.call(this, id, clone(fields), before);
            }
  
            // This line triggers if we provide added with movedBefore.
            if (callbacks.added) {
              callbacks.added.call(this, id, clone(fields));
            }
  
            // XXX could `before` be a falsy ID?  Technically
            // idStringify seems to allow for them -- though
            // OrderedDict won't call stringify on a falsy arg.
            (this.docs as OrderedDict).putBefore(id, doc, before || null);
          },
          movedBefore: (id, before) => {
            const doc = this.docs.get(id);
  
            if (callbacks.movedBefore) {
              callbacks.movedBefore.call(this, id, before);
            }
  
            (this.docs as OrderedDict).moveBefore(id, before || null);
          },
        };
      } else {
        this.docs = new IdMap();
        this.applyChange = {
          added: (id, fields) => {
            // Take a shallow copy since the top-level properties can be changed
            const doc = { ...fields };
  
            if (callbacks.added) {
              callbacks.added.call(this, id, clone(fields));
            }
  
            doc._id = id;
  
            (this.docs as IdMap).set(id,  doc);
          },
        };
      }
  
      // The methods in _IdMap and OrderedDict used by these callbacks are
      // identical.
      this.applyChange.changed = (id, fields) => {
        const doc = this.docs.get(id);
  
        if (!doc) {
          throw new Error(`Unknown id for changed: ${id}`);
        }
  
        if (callbacks.changed) {
          callbacks.changed.call(this, id, clone(fields));
        }
  
        DiffSequence.applyChanges(doc, fields);
      };
  
      this.applyChange.removed = id => {
        if (callbacks.removed) {
          callbacks.removed.call(this, id);
        }
  
        (this.docs as IdMap).remove(id);
      };
    }
  };