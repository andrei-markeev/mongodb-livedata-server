// _CachingChangeObserver is an object which receives observeChanges callbacks
// and keeps a cache of the current cursor state up to date in this.docs. Users
// of this class should read the docs field but not modify it. You should pass
// the "applyChange" field as the callbacks to the underlying observeChanges
// call. Optionally, you can specify your own observeChanges callbacks which are
// invoked immediately before the docs field is updated; this object is made

import { DiffSequence } from "../diff-sequence/diff";
import { OrderedDict } from "../ordered-dict/ordered_dict";

// available as `this` to those callbacks.
export class _CachingChangeObserver {
    public docs: OrderedDict | Map<string, any>;
    public applyChange: {
        initialAdds?: (docs: OrderedDict | Map<string, any>) => void;
        added?: (id: string, fields: any) => void;
        changed?: (id: string, fields: any) => void;
        removed?: (id: string) => void;
        addedBefore?: (id: string, fields: any, before: any) => void;
        movedBefore?: (id: string, before: any) => void;
    };

    private ordered: boolean;

    constructor(options: { ordered?: boolean } = {}) {
      this.ordered = options.ordered || false;
      if (this.ordered) {
        this.docs = new OrderedDict();
        this.applyChange = {
          addedBefore: (id, fields, before) => {
            // Take a shallow copy since the top-level properties can be changed
            const doc = { ...fields };
  
            doc._id = id;
  
            // XXX could `before` be a falsy ID?  Technically
            // idStringify seems to allow for them -- though
            // OrderedDict won't call stringify on a falsy arg.
            (this.docs as OrderedDict).putBefore(id, doc, before || null);
          },
          movedBefore: (id, before) => {
            const doc = this.docs.get(id);
  
            (this.docs as OrderedDict).moveBefore(id, before || null);
          },
        };
      } else {
        this.docs = new Map();
        this.applyChange = {
          added: (id, fields) => {
            // Take a shallow copy since the top-level properties can be changed
            const doc = { ...fields };
  
            doc._id = id;
  
            (this.docs as Map<string, any>).set(id,  doc);
          },
        };
      }

      this.applyChange.initialAdds = (docs) => {
        this.docs = docs;
      };

      // The methods in _IdMap and OrderedDict used by these callbacks are
      // identical.
      this.applyChange.changed = (id, fields) => {
        const doc = this.docs.get(id);
  
        if (!doc) {
          throw new Error(`Unknown id for changed: ${id}`);
        }
  
        DiffSequence.applyChanges(doc, fields);
      };
  
      this.applyChange.removed = id => {
        this.docs.delete(id);
      };
    }
  };