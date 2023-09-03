import {
    combineImportantPathsIntoProjection,
    compileDocumentSelector,
    hasOwn,
    isNumericKey,
    isOperatorObject,
    nothingMatcher,
    _modify,
    _pathsElidingNumericKeys,
    pathsToTree,
} from './minimongo_common';
import { Filter } from 'mongodb';
import { clone, isBinary } from '../ejson/ejson';

// The minimongo selector compiler!

// Terminology:
//  - a 'selector' is the EJSON object representing a selector
//  - a 'matcher' is its compiled form (whether a full Minimongo.Matcher
//    object or one of the component lambdas that matches parts of it)
//  - a 'result object' is an object with a 'result' field and maybe
//    distance and arrayIndices.
//  - a 'branched value' is an object with a 'value' field and maybe
//    'dontIterate' and 'arrayIndices'.
//  - a 'document' is a top-level object that can be stored in a collection.
//  - a 'lookup function' is a function that takes in a document and returns
//    an array of 'branched values'.
//  - a 'branched matcher' maps from an array of branched values to a result
//    object.
//  - an 'element matcher' maps from a single value to a bool.

// Main entry point.
//   var matcher = new Minimongo.Matcher({a: {$gt: 5}});
//   if (matcher.documentMatches({a: 7})) ...

export class MinimongoMatcher {
    private _paths: Record<string, any>;
    private _hasGeoQuery: boolean;
    private _hasWhere: boolean;
    private _isSimple: boolean;
    private _matchingDocument: any;
    private _selector: Filter<any>;
    private _docMatcher: any;
    private _isUpdate: boolean;

    constructor(selector: Filter<any>, isUpdate?: boolean) {
        // A set (object mapping string -> *) of all of the document paths looked
        // at by the selector. Also includes the empty string if it may look at any
        // path (eg, $where).
        this._paths = {};
        // Set to true if compilation finds a $near.
        this._hasGeoQuery = false;
        // Set to true if compilation finds a $where.
        this._hasWhere = false;
        // Set to false if compilation finds anything other than a simple equality
        // or one or more of '$gt', '$gte', '$lt', '$lte', '$ne', '$in', '$nin' used
        // with scalars as operands.
        this._isSimple = true;
        // Set to a dummy document which always matches this Matcher. Or set to null
        // if such document is too hard to find.
        this._matchingDocument = undefined;
        // A clone of the original selector. It may just be a function if the user
        // passed in a function; otherwise is definitely an object (eg, IDs are
        // translated into {_id: ID} first. Used by canBecomeTrueByModifier and
        // Sorter._useWithMatcher.
        this._selector = null;
        this._docMatcher = this._compileSelector(selector);
        // Set to true if selection is done for an update operation
        // Default is false
        // Used for $near array update (issue #3599)
        this._isUpdate = isUpdate;
    }

    documentMatches(doc) {
        if (doc !== Object(doc)) {
            throw Error('documentMatches needs a document');
        }

        return this._docMatcher(doc);
    }

    hasGeoQuery() {
        return this._hasGeoQuery;
    }

    hasWhere() {
        return this._hasWhere;
    }

    isSimple() {
        return this._isSimple;
    }

    // Given a selector, return a function that takes one argument, a
    // document. It returns a result object.
    _compileSelector(selector: Filter<any>) {
        // you can pass a literal function instead of a selector
        if (selector instanceof Function) {
            this._isSimple = false;
            this._selector = selector;
            this._recordPathUsed('');

            return doc => ({ result: !!selector.call(doc) });
        }

        // protect against dangerous selectors.  falsey and {_id: falsey} are both
        // likely programmer error, and not what you want, particularly for
        // destructive operations.
        if (!selector || hasOwn.call(selector, '_id') && !selector._id) {
            this._isSimple = false;
            return nothingMatcher;
        }

        // Top level can't be an array or true or binary.
        if (Array.isArray(selector) ||
            isBinary(selector) ||
            typeof selector === 'boolean') {
            throw new Error(`Invalid selector: ${selector}`);
        }

        this._selector = clone(selector);

        return compileDocumentSelector(selector, this, { isRoot: true });
    }

    affectedByModifier(modifier) {
        // safe check for $set/$unset being objects
        modifier = Object.assign({ $set: {}, $unset: {} }, modifier);

        const meaningfulPaths = this._getPaths();
        const modifiedPaths = [].concat(
            Object.keys(modifier.$set),
            Object.keys(modifier.$unset)
        );

        return modifiedPaths.some(path => {
            const mod = path.split('.');

            return meaningfulPaths.some(meaningfulPath => {
                const sel = meaningfulPath.split('.');

                let i = 0, j = 0;

                while (i < sel.length && j < mod.length) {
                    if (isNumericKey(sel[i]) && isNumericKey(mod[j])) {
                        // foo.4.bar selector affected by foo.4 modifier
                        // foo.3.bar selector unaffected by foo.4 modifier
                        if (sel[i] === mod[j]) {
                            i++;
                            j++;
                        } else {
                            return false;
                        }
                    } else if (isNumericKey(sel[i])) {
                        // foo.4.bar selector unaffected by foo.bar modifier
                        return false;
                    } else if (isNumericKey(mod[j])) {
                        j++;
                    } else if (sel[i] === mod[j]) {
                        i++;
                        j++;
                    } else {
                        return false;
                    }
                }

                // One is a prefix of another, taking numeric fields into account
                return true;
            });
        });
    }

    canBecomeTrueByModifier(modifier) {
        if (!this.affectedByModifier(modifier)) {
            return false;
        }

        if (!this.isSimple()) {
            return true;
        }

        modifier = Object.assign({ $set: {}, $unset: {} }, modifier);

        const modifierPaths = [].concat(
            Object.keys(modifier.$set),
            Object.keys(modifier.$unset)
        );

        if (this._getPaths().some(pathHasNumericKeys) ||
            modifierPaths.some(pathHasNumericKeys)) {
            return true;
        }

        // check if there is a $set or $unset that indicates something is an
        // object rather than a scalar in the actual object where we saw $-operator
        // NOTE: it is correct since we allow only scalars in $-operators
        // Example: for selector {'a.b': {$gt: 5}} the modifier {'a.b.c':7} would
        // definitely set the result to false as 'a.b' appears to be an object.
        const expectedScalarIsObject = Object.keys(this._selector).some(path => {
            if (!isOperatorObject(this._selector[path])) {
                return false;
            }

            return modifierPaths.some(modifierPath =>
                modifierPath.startsWith(`${path}.`)
            );
        });

        if (expectedScalarIsObject) {
            return false;
        }

        // See if we can apply the modifier on the ideally matching object. If it
        // still matches the selector, then the modifier could have turned the real
        // object in the database into something matching.
        const matchingDocument = clone(this.matchingDocument());

        // The selector is too complex, anything can happen.
        if (matchingDocument === null) {
            return true;
        }

        try {
            _modify(matchingDocument, modifier);
        } catch (error) {
            // Couldn't set a property on a field which is a scalar or null in the
            // selector.
            // Example:
            // real document: { 'a.b': 3 }
            // selector: { 'a': 12 }
            // converted selector (ideal document): { 'a': 12 }
            // modifier: { $set: { 'a.b': 4 } }
            // We don't know what real document was like but from the error raised by
            // $set on a scalar field we can reason that the structure of real document
            // is completely different.
            if (error.name === 'MinimongoError' && error.setPropertyError) {
                return false;
            }

            throw error;
        }

        return this.documentMatches(matchingDocument).result;
    }

    matchingDocument() {
        // check if it was computed before
        if (this._matchingDocument !== undefined) {
          return this._matchingDocument;
        }

        // If the analysis of this selector is too hard for our implementation
        // fallback to "YES"
        let fallback = false;

        this._matchingDocument = pathsToTree(
          this._getPaths(),
          path => {
            const valueSelector = this._selector[path];

            if (isOperatorObject(valueSelector)) {
              // if there is a strict equality, there is a good
              // chance we can use one of those as "matching"
              // dummy value
              if (valueSelector.$eq) {
                return valueSelector.$eq;
              }
      
              if (valueSelector.$in) {
                const matcher = new MinimongoMatcher({placeholder: valueSelector});
      
                // Return anything from $in that matches the whole selector for this
                // path. If nothing matches, returns `undefined` as nothing can make
                // this selector into `true`.
                return valueSelector.$in.find(placeholder =>
                  matcher.documentMatches({placeholder}).result
                );
              }

              if (onlyContainsKeys(valueSelector, ['$gt', '$gte', '$lt', '$lte'])) {
                let lowerBound = -Infinity;
                let upperBound = Infinity;

                ['$lte', '$lt'].forEach(op => {
                  if (hasOwn.call(valueSelector, op) &&
                      valueSelector[op] < upperBound) {
                    upperBound = valueSelector[op];
                  }
                });

                ['$gte', '$gt'].forEach(op => {
                  if (hasOwn.call(valueSelector, op) &&
                      valueSelector[op] > lowerBound) {
                    lowerBound = valueSelector[op];
                  }
                });

                const middle = (lowerBound + upperBound) / 2;
                const matcher = new MinimongoMatcher({placeholder: valueSelector});

                if (!matcher.documentMatches({placeholder: middle}).result &&
                    (middle === lowerBound || middle === upperBound)) {
                  fallback = true;
                }

                return middle;
              }

              if (onlyContainsKeys(valueSelector, ['$nin', '$ne'])) {
                // Since this._isSimple makes sure $nin and $ne are not combined with
                // objects or arrays, we can confidently return an empty object as it
                // never matches any scalar.
                return {};
              }

              fallback = true;
            }

            return this._selector[path];
          },
          x => x);

        if (fallback) {
          this._matchingDocument = null;
        }

        return this._matchingDocument;
    };

    // Knows how to combine a mongo selector and a fields projection to a new fields
    // projection taking into account active fields from the passed selector.
    // @returns Object - projection object (same as fields option of mongo cursor)
    combineIntoProjection(projection) {
        const selectorPaths = _pathsElidingNumericKeys(this._getPaths());
    
        // Special case for $where operator in the selector - projection should depend
        // on all fields of the document. getSelectorPaths returns a list of paths
        // selector depends on. If one of the paths is '' (empty string) representing
        // the root or the whole document, complete projection should be returned.
        if (selectorPaths.includes('')) {
            return {};
        }
    
        return combineImportantPathsIntoProjection(selectorPaths, projection);
    }

    // Returns a list of key paths the given selector is looking for. It includes
    // the empty string if there is a $where.
    _getPaths() {
        return Object.keys(this._paths);
    }

    _recordPathUsed(path) {
        this._paths[path] = true;
    }
}

function pathHasNumericKeys(path: string) {
    return path.split('.').some(isNumericKey);
}

function onlyContainsKeys(obj, keys) {
    return Object.keys(obj).every(k => keys.includes(k));
}
