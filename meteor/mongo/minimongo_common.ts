import { Decimal128, ObjectId } from "mongodb";
import { clone, equals, isBinary } from "../ejson/ejson";
import { geometryWithinRadius, pointDistance } from "./geojson_utils";
import { MinimongoMatcher } from "./minimongo_matcher";
import MinimongoSorter from "./minimongo_sorter";

export const hasOwn = Object.prototype.hasOwnProperty;

// Each element selector contains:
//  - compileElementSelector, a function with args:
//    - operand - the "right hand side" of the operator
//    - valueSelector - the "context" for the operator (so that $regex can find
//      $options)
//    - matcher - the Matcher this is going into (so that $elemMatch can compile
//      more things)
//    returning a function mapping a single value to bool.
//  - dontExpandLeafArrays, a bool which prevents expandArraysInBranches from
//    being called
//  - dontIncludeLeafArrays, a bool which causes an argument to be passed to
//    expandArraysInBranches if it is called
export const ELEMENT_OPERATORS = {
    $lt: makeInequality(cmpValue => cmpValue < 0),
    $gt: makeInequality(cmpValue => cmpValue > 0),
    $lte: makeInequality(cmpValue => cmpValue <= 0),
    $gte: makeInequality(cmpValue => cmpValue >= 0),
    $mod: {
        compileElementSelector(operand) {
            if (!(Array.isArray(operand) && operand.length === 2
                && typeof operand[0] === 'number'
                && typeof operand[1] === 'number')) {
                throw Error('argument to $mod must be an array of two numbers');
            }

            // XXX could require to be ints or round or something
            const divisor = operand[0];
            const remainder = operand[1];
            return value => (
                typeof value === 'number' && value % divisor === remainder
            );
        },
    },
    $in: {
        compileElementSelector(operand) {
            if (!Array.isArray(operand)) {
                throw Error('$in needs an array');
            }

            const elementMatchers = operand.map(option => {
                if (option instanceof RegExp) {
                    return regexpElementMatcher(option);
                }

                if (isOperatorObject(option)) {
                    throw Error('cannot nest $ under $in');
                }

                return equalityElementMatcher(option);
            });

            return value => {
                // Allow {a: {$in: [null]}} to match when 'a' does not exist.
                if (value === undefined) {
                    value = null;
                }

                return elementMatchers.some(matcher => matcher(value));
            };
        },
    },
    $size: {
        // {a: [[5, 5]]} must match {a: {$size: 1}} but not {a: {$size: 2}}, so we
        // don't want to consider the element [5,5] in the leaf array [[5,5]] as a
        // possible value.
        dontExpandLeafArrays: true,
        compileElementSelector(operand) {
            if (typeof operand === 'string') {
                // Don't ask me why, but by experimentation, this seems to be what Mongo
                // does.
                operand = 0;
            } else if (typeof operand !== 'number') {
                throw Error('$size needs a number');
            }

            return value => Array.isArray(value) && value.length === operand;
        },
    },
    $type: {
        // {a: [5]} must not match {a: {$type: 4}} (4 means array), but it should
        // match {a: {$type: 1}} (1 means number), and {a: [[5]]} must match {$a:
        // {$type: 4}}. Thus, when we see a leaf array, we *should* expand it but
        // should *not* include it itself.
        dontIncludeLeafArrays: true,
        compileElementSelector(operand) {
            if (typeof operand === 'string') {
                const operandAliasMap = {
                    'double': 1,
                    'string': 2,
                    'object': 3,
                    'array': 4,
                    'binData': 5,
                    'undefined': 6,
                    'objectId': 7,
                    'bool': 8,
                    'date': 9,
                    'null': 10,
                    'regex': 11,
                    'dbPointer': 12,
                    'javascript': 13,
                    'symbol': 14,
                    'javascriptWithScope': 15,
                    'int': 16,
                    'timestamp': 17,
                    'long': 18,
                    'decimal': 19,
                    'minKey': -1,
                    'maxKey': 127,
                };
                if (!hasOwn.call(operandAliasMap, operand)) {
                    throw Error(`unknown string alias for $type: ${operand}`);
                }
                operand = operandAliasMap[operand];
            } else if (typeof operand === 'number') {
                if (operand === 0 || operand < -1
                    || (operand > 19 && operand !== 127)) {
                    throw Error(`Invalid numerical $type code: ${operand}`);
                }
            } else {
                throw Error('argument to $type is not a number or a string');
            }

            return value => (
                value !== undefined && _f._type(value) === operand
            );
        },
    },
    $bitsAllSet: {
        compileElementSelector(operand) {
            const mask = getOperandBitmask(operand, '$bitsAllSet');
            return value => {
                const bitmask = getValueBitmask(value, mask.length);
                return bitmask && mask.every((byte, i) => (bitmask[i] & byte) === byte);
            };
        },
    },
    $bitsAnySet: {
        compileElementSelector(operand) {
            const mask = getOperandBitmask(operand, '$bitsAnySet');
            return value => {
                const bitmask = getValueBitmask(value, mask.length);
                return bitmask && mask.some((byte, i) => (~bitmask[i] & byte) !== byte);
            };
        },
    },
    $bitsAllClear: {
        compileElementSelector(operand) {
            const mask = getOperandBitmask(operand, '$bitsAllClear');
            return value => {
                const bitmask = getValueBitmask(value, mask.length);
                return bitmask && mask.every((byte, i) => !(bitmask[i] & byte));
            };
        },
    },
    $bitsAnyClear: {
        compileElementSelector(operand) {
            const mask = getOperandBitmask(operand, '$bitsAnyClear');
            return value => {
                const bitmask = getValueBitmask(value, mask.length);
                return bitmask && mask.some((byte, i) => (bitmask[i] & byte) !== byte);
            };
        },
    },
    $regex: {
        compileElementSelector(operand, valueSelector) {
            if (!(typeof operand === 'string' || operand instanceof RegExp)) {
                throw Error('$regex has to be a string or RegExp');
            }

            let regexp;
            if (valueSelector.$options !== undefined) {
                // Options passed in $options (even the empty string) always overrides
                // options in the RegExp object itself.

                // Be clear that we only support the JS-supported options, not extended
                // ones (eg, Mongo supports x and s). Ideally we would implement x and s
                // by transforming the regexp, but not today...
                if (/[^gim]/.test(valueSelector.$options)) {
                    throw new Error('Only the i, m, and g regexp options are supported');
                }

                const source = operand instanceof RegExp ? operand.source : operand;
                regexp = new RegExp(source, valueSelector.$options);
            } else if (operand instanceof RegExp) {
                regexp = operand;
            } else {
                regexp = new RegExp(operand);
            }

            return regexpElementMatcher(regexp);
        },
    },
    $elemMatch: {
        dontExpandLeafArrays: true,
        compileElementSelector(operand, valueSelector, matcher) {
            if (!_isPlainObject(operand)) {
                throw Error('$elemMatch need an object');
            }

            const isDocMatcher = !isOperatorObject(
                Object.keys(operand)
                    .filter(key => !hasOwn.call(LOGICAL_OPERATORS, key))
                    .reduce((a, b) => Object.assign(a, { [b]: operand[b] }), {}),
                true);

            let subMatcher;
            if (isDocMatcher) {
                // This is NOT the same as compileValueSelector(operand), and not just
                // because of the slightly different calling convention.
                // {$elemMatch: {x: 3}} means "an element has a field x:3", not
                // "consists only of a field x:3". Also, regexps and sub-$ are allowed.
                subMatcher =
                    compileDocumentSelector(operand, matcher, { inElemMatch: true });
            } else {
                subMatcher = compileValueSelector(operand, matcher);
            }

            return value => {
                if (!Array.isArray(value)) {
                    return false;
                }

                for (let i = 0; i < value.length; ++i) {
                    const arrayElement = value[i];
                    let arg;
                    if (isDocMatcher) {
                        // We can only match {$elemMatch: {b: 3}} against objects.
                        // (We can also match against arrays, if there's numeric indices,
                        // eg {$elemMatch: {'0.b': 3}} or {$elemMatch: {0: 3}}.)
                        if (!isIndexable(arrayElement)) {
                            return false;
                        }

                        arg = arrayElement;
                    } else {
                        // dontIterate ensures that {a: {$elemMatch: {$gt: 5}}} matches
                        // {a: [8]} but not {a: [[8]]}
                        arg = [{ value: arrayElement, dontIterate: true }];
                    }
                    // XXX support $near in $elemMatch by propagating $distance?
                    if (subMatcher(arg).result) {
                        return i; // specially understood to mean "use as arrayIndices"
                    }
                }

                return false;
            };
        },
    },
};

// Operators that appear at the top level of a document selector.
const LOGICAL_OPERATORS = {
    $and(subSelector, matcher, inElemMatch) {
        return andDocumentMatchers(
            compileArrayOfDocumentSelectors(subSelector, matcher, inElemMatch)
        );
    },

    $or(subSelector, matcher, inElemMatch) {
        const matchers = compileArrayOfDocumentSelectors(
            subSelector,
            matcher,
            inElemMatch
        );

        // Special case: if there is only one matcher, use it directly, *preserving*
        // any arrayIndices it returns.
        if (matchers.length === 1) {
            return matchers[0];
        }

        return doc => {
            const result = matchers.some(fn => fn(doc).result);
            // $or does NOT set arrayIndices when it has multiple
            // sub-expressions. (Tested against MongoDB.)
            return { result };
        };
    },

    $nor(subSelector, matcher, inElemMatch) {
        const matchers = compileArrayOfDocumentSelectors(
            subSelector,
            matcher,
            inElemMatch
        );
        return doc => {
            const result = matchers.every(fn => !fn(doc).result);
            // Never set arrayIndices, because we only match if nothing in particular
            // 'matched' (and because this is consistent with MongoDB).
            return { result };
        };
    },

    $where(selectorValue, matcher) {
        // Record that *any* path may be used.
        matcher._recordPathUsed('');
        matcher._hasWhere = true;

        if (!(selectorValue instanceof Function)) {
            // XXX MongoDB seems to have more complex logic to decide where or or not
            // to add 'return'; not sure exactly what it is.
            selectorValue = Function('obj', `return ${selectorValue}`);
        }

        // We make the document available as both `this` and `obj`.
        // // XXX not sure what we should do if this throws
        return doc => ({ result: selectorValue.call(doc, doc) });
    },

    // This is just used as a comment in the query (in MongoDB, it also ends up in
    // query logs); it has no effect on the actual selection.
    $comment() {
        return () => ({ result: true });
    },
};

// Operators that (unlike LOGICAL_OPERATORS) pertain to individual paths in a
// document, but (unlike ELEMENT_OPERATORS) do not have a simple definition as
// "match each branched value independently and combine with
// convertElementMatcherToBranchedMatcher".
const VALUE_OPERATORS = {
    $eq(operand) {
        return convertElementMatcherToBranchedMatcher(
            equalityElementMatcher(operand)
        );
    },
    $not(operand, valueSelector, matcher) {
        return invertBranchedMatcher(compileValueSelector(operand, matcher));
    },
    $ne(operand) {
        return invertBranchedMatcher(
            convertElementMatcherToBranchedMatcher(equalityElementMatcher(operand))
        );
    },
    $nin(operand) {
        return invertBranchedMatcher(
            convertElementMatcherToBranchedMatcher(
                ELEMENT_OPERATORS.$in.compileElementSelector(operand)
            )
        );
    },
    $exists(operand) {
        const exists = convertElementMatcherToBranchedMatcher(
            value => value !== undefined
        );
        return operand ? exists : invertBranchedMatcher(exists);
    },
    // $options just provides options for $regex; its logic is inside $regex
    $options(operand, valueSelector) {
        if (!hasOwn.call(valueSelector, '$regex')) {
            throw Error('$options needs a $regex');
        }

        return everythingMatcher;
    },
    // $maxDistance is basically an argument to $near
    $maxDistance(operand, valueSelector) {
        if (!valueSelector.$near) {
            throw Error('$maxDistance needs a $near');
        }

        return everythingMatcher;
    },
    $all(operand, valueSelector, matcher) {
        if (!Array.isArray(operand)) {
            throw Error('$all requires array');
        }

        // Not sure why, but this seems to be what MongoDB does.
        if (operand.length === 0) {
            return nothingMatcher;
        }

        const branchedMatchers = operand.map(criterion => {
            // XXX handle $all/$elemMatch combination
            if (isOperatorObject(criterion)) {
                throw Error('no $ expressions in $all');
            }

            // This is always a regexp or equality selector.
            return compileValueSelector(criterion, matcher);
        });

        // andBranchedMatchers does NOT require all selectors to return true on the
        // SAME branch.
        return andBranchedMatchers(branchedMatchers);
    },
    $near(operand, valueSelector, matcher, isRoot) {
        if (!isRoot) {
            throw Error('$near can\'t be inside another $ operator');
        }

        matcher._hasGeoQuery = true;

        // There are two kinds of geodata in MongoDB: legacy coordinate pairs and
        // GeoJSON. They use different distance metrics, too. GeoJSON queries are
        // marked with a $geometry property, though legacy coordinates can be
        // matched using $geometry.
        let maxDistance, point, distance;
        if (_isPlainObject(operand) && hasOwn.call(operand, '$geometry')) {
            // GeoJSON "2dsphere" mode.
            maxDistance = operand.$maxDistance;
            point = operand.$geometry;
            distance = value => {
                // XXX: for now, we don't calculate the actual distance between, say,
                // polygon and circle. If people care about this use-case it will get
                // a priority.
                if (!value) {
                    return null;
                }

                if (!value.type) {
                    return pointDistance(
                        point,
                        { type: 'Point', coordinates: pointToArray(value) }
                    );
                }

                if (value.type === 'Point') {
                    return pointDistance(point, value);
                }

                return geometryWithinRadius(value, point, maxDistance)
                    ? 0
                    : maxDistance + 1;
            };
        } else {
            maxDistance = valueSelector.$maxDistance;

            if (!isIndexable(operand)) {
                throw Error('$near argument must be coordinate pair or GeoJSON');
            }

            point = pointToArray(operand);

            distance = value => {
                if (!isIndexable(value)) {
                    return null;
                }

                return distanceCoordinatePairs(point, value);
            };
        }

        return branchedValues => {
            // There might be multiple points in the document that match the given
            // field. Only one of them needs to be within $maxDistance, but we need to
            // evaluate all of them and use the nearest one for the implicit sort
            // specifier. (That's why we can't just use ELEMENT_OPERATORS here.)
            //
            // Note: This differs from MongoDB's implementation, where a document will
            // actually show up *multiple times* in the result set, with one entry for
            // each within-$maxDistance branching point.
            const result = { result: false, distance: undefined, arrayIndices: undefined };
            expandArraysInBranches(branchedValues).every(branch => {
                // if operation is an update, don't skip branches, just return the first
                // one (#3599)
                let curDistance;
                if (!matcher._isUpdate) {
                    if (!(typeof branch.value === 'object')) {
                        return true;
                    }

                    curDistance = distance(branch.value);

                    // Skip branches that aren't real points or are too far away.
                    if (curDistance === null || curDistance > maxDistance) {
                        return true;
                    }

                    // Skip anything that's a tie.
                    if (result.distance !== undefined && result.distance <= curDistance) {
                        return true;
                    }
                }

                result.result = true;
                result.distance = curDistance;

                if (branch.arrayIndices) {
                    result.arrayIndices = branch.arrayIndices;
                } else {
                    delete result.arrayIndices;
                }

                return !matcher._isUpdate;
            });

            return result;
        };
    },
};

// NB: We are cheating and using this function to implement 'AND' for both
// 'document matchers' and 'branched matchers'. They both return result objects
// but the argument is different: for the former it's a whole doc, whereas for
// the latter it's an array of 'branched values'.
function andSomeMatchers(subMatchers) {
    if (subMatchers.length === 0) {
        return everythingMatcher;
    }

    if (subMatchers.length === 1) {
        return subMatchers[0];
    }

    return docOrBranches => {
        const match: { result?: boolean, distance?: number, arrayIndices?: number[] } = {};
        match.result = subMatchers.every(fn => {
            const subResult = fn(docOrBranches);

            // Copy a 'distance' number out of the first sub-matcher that has
            // one. Yes, this means that if there are multiple $near fields in a
            // query, something arbitrary happens; this appears to be consistent with
            // Mongo.
            if (subResult.result &&
                subResult.distance !== undefined &&
                match.distance === undefined) {
                match.distance = subResult.distance;
            }

            // Similarly, propagate arrayIndices from sub-matchers... but to match
            // MongoDB behavior, this time the *last* sub-matcher with arrayIndices
            // wins.
            if (subResult.result && subResult.arrayIndices) {
                match.arrayIndices = subResult.arrayIndices;
            }

            return subResult.result;
        });

        // If we didn't actually match, forget any extra metadata we came up with.
        if (!match.result) {
            delete match.distance;
            delete match.arrayIndices;
        }

        return match;
    };
}

const andDocumentMatchers = andSomeMatchers;
const andBranchedMatchers = andSomeMatchers;

function compileArrayOfDocumentSelectors(selectors, matcher, inElemMatch) {
    if (!Array.isArray(selectors) || selectors.length === 0) {
        throw Error('$and/$or/$nor must be nonempty array');
    }

    return selectors.map(subSelector => {
        if (!_isPlainObject(subSelector)) {
            throw Error('$or/$and/$nor entries need to be full objects');
        }

        return compileDocumentSelector(subSelector, matcher, { inElemMatch });
    });
}

// Takes in a selector that could match a full document (eg, the original
// selector). Returns a function mapping document->result object.
//
// matcher is the Matcher object we are compiling.
//
// If this is the root document selector (ie, not wrapped in $and or the like),
// then isRoot is true. (This is used by $near.)
export function compileDocumentSelector(docSelector, matcher, options: { isRoot?: boolean, inElemMatch?: boolean } = {}) {
    const docMatchers = Object.keys(docSelector).map(key => {
        const subSelector = docSelector[key];

        if (key.substr(0, 1) === '$') {
            // Outer operators are either logical operators (they recurse back into
            // this function), or $where.
            if (!hasOwn.call(LOGICAL_OPERATORS, key)) {
                throw new Error(`Unrecognized logical operator: ${key}`);
            }

            matcher._isSimple = false;
            return LOGICAL_OPERATORS[key](subSelector, matcher, options.inElemMatch);
        }

        // Record this path, but only if we aren't in an elemMatcher, since in an
        // elemMatch this is a path inside an object in an array, not in the doc
        // root.
        if (!options.inElemMatch) {
            matcher._recordPathUsed(key);
        }

        // Don't add a matcher if subSelector is a function -- this is to match
        // the behavior of Meteor on the server (inherited from the node mongodb
        // driver), which is to ignore any part of a selector which is a function.
        if (typeof subSelector === 'function') {
            return undefined;
        }

        const lookUpByIndex = makeLookupFunction(key);
        const valueMatcher = compileValueSelector(
            subSelector,
            matcher,
            options.isRoot
        );

        return doc => valueMatcher(lookUpByIndex(doc));
    }).filter(Boolean);

    return andDocumentMatchers(docMatchers);
}

// Takes in a selector that could match a key-indexed value in a document; eg,
// {$gt: 5, $lt: 9}, or a regular expression, or any non-expression object (to
// indicate equality).  Returns a branched matcher: a function mapping
// [branched value]->result object.
function compileValueSelector(valueSelector, matcher, isRoot?: boolean) {
    if (valueSelector instanceof RegExp) {
        matcher._isSimple = false;
        return convertElementMatcherToBranchedMatcher(
            regexpElementMatcher(valueSelector)
        );
    }

    if (isOperatorObject(valueSelector)) {
        return operatorBranchedMatcher(valueSelector, matcher, isRoot);
    }

    return convertElementMatcherToBranchedMatcher(
        equalityElementMatcher(valueSelector)
    );
}

// Given an element matcher (which evaluates a single value), returns a branched
// value (which evaluates the element matcher on all the branches and returns a
// more structured return value possibly including arrayIndices).
function convertElementMatcherToBranchedMatcher(elementMatcher, options: { dontExpandLeafArrays?: boolean, dontIncludeLeafArrays?: boolean } = {}) {
    return (branches: any[]) => {
        const expanded = options.dontExpandLeafArrays
            ? branches
            : expandArraysInBranches(branches, options.dontIncludeLeafArrays);

        const match = { result: false, arrayIndices: [] };
        match.result = expanded.some(element => {
            let matched = elementMatcher(element.value);

            // Special case for $elemMatch: it means "true, and use this as an array
            // index if I didn't already have one".
            if (typeof matched === 'number') {
                // XXX This code dates from when we only stored a single array index
                // (for the outermost array). Should we be also including deeper array
                // indices from the $elemMatch match?
                if (!element.arrayIndices) {
                    element.arrayIndices = [matched];
                }

                matched = true;
            }

            // If some element matched, and it's tagged with array indices, include
            // those indices in our result object.
            if (matched && element.arrayIndices) {
                match.arrayIndices = element.arrayIndices;
            }

            return matched;
        });

        return match;
    };
}

// Helpers for $near.
function distanceCoordinatePairs(a, b) {
    const pointA = pointToArray(a);
    const pointB = pointToArray(b);

    return Math.hypot(pointA[0] - pointB[0], pointA[1] - pointB[1]);
}

// Takes something that is not an operator object and returns an element matcher
// for equality with that thing.
export function equalityElementMatcher(elementSelector) {
    if (isOperatorObject(elementSelector)) {
        throw Error('Can\'t create equalityValueSelector for operator object');
    }

    // Special-case: null and undefined are equal (if you got undefined in there
    // somewhere, or if you got it due to some branch being non-existent in the
    // weird special case), even though they aren't with EJSON.equals.
    // undefined or null
    if (elementSelector == null) {
        return value => value == null;
    }

    return value => _f._equal(elementSelector, value);
}

function everythingMatcher(docOrBranchedValues) {
    return { result: true };
}

export function expandArraysInBranches(branches, skipTheArrays?: boolean) {
    const branchesOut = [];

    branches.forEach(branch => {
        const thisIsArray = Array.isArray(branch.value);

        // We include the branch itself, *UNLESS* we it's an array that we're going
        // to iterate and we're told to skip arrays.  (That's right, we include some
        // arrays even skipTheArrays is true: these are arrays that were found via
        // explicit numerical indices.)
        if (!(skipTheArrays && thisIsArray && !branch.dontIterate)) {
            branchesOut.push({ arrayIndices: branch.arrayIndices, value: branch.value });
        }

        if (thisIsArray && !branch.dontIterate) {
            branch.value.forEach((value, i) => {
                branchesOut.push({
                    arrayIndices: (branch.arrayIndices || []).concat(i),
                    value
                });
            });
        }
    });

    return branchesOut;
}

// Helpers for $bitsAllSet/$bitsAnySet/$bitsAllClear/$bitsAnyClear.
function getOperandBitmask(operand, selector) {
    // numeric bitmask
    // You can provide a numeric bitmask to be matched against the operand field.
    // It must be representable as a non-negative 32-bit signed integer.
    // Otherwise, $bitsAllSet will return an error.
    if (Number.isInteger(operand) && operand >= 0) {
        return new Uint8Array(new Int32Array([operand]).buffer);
    }

    // bindata bitmask
    // You can also use an arbitrarily large BinData instance as a bitmask.
    if (isBinary(operand)) {
        return new Uint8Array(operand.buffer);
    }

    // position list
    // If querying a list of bit positions, each <position> must be a non-negative
    // integer. Bit positions start at 0 from the least significant bit.
    if (Array.isArray(operand) &&
        operand.every(x => Number.isInteger(x) && x >= 0)) {
        const buffer = new ArrayBuffer((Math.max(...operand) >> 3) + 1);
        const view = new Uint8Array(buffer);

        operand.forEach(x => {
            view[x >> 3] |= 1 << (x & 0x7);
        });

        return view;
    }

    // bad operand
    throw Error(
        `operand to ${selector} must be a numeric bitmask (representable as a ` +
        'non-negative 32-bit signed integer), a bindata bitmask or an array with ' +
        'bit positions (non-negative integers)'
    );
}

function getValueBitmask(value, length) {
    // The field value must be either numerical or a BinData instance. Otherwise,
    // $bits... will not match the current document.

    // numerical
    if (Number.isSafeInteger(value)) {
        // $bits... will not match numerical values that cannot be represented as a
        // signed 64-bit integer. This can be the case if a value is either too
        // large or small to fit in a signed 64-bit integer, or if it has a
        // fractional component.
        const buffer = new ArrayBuffer(
            Math.max(length, 2 * Uint32Array.BYTES_PER_ELEMENT)
        );

        let view32 = new Uint32Array(buffer, 0, 2);
        view32[0] = value % ((1 << 16) * (1 << 16)) | 0;
        view32[1] = value / ((1 << 16) * (1 << 16)) | 0;

        // sign extension
        if (value < 0) {
            const view8 = new Uint8Array(buffer, 2);
            view8.forEach((byte, i) => {
                view8[i] = 0xff;
            });
        }

        return new Uint8Array(buffer);
    }

    // bindata
    if (isBinary(value)) {
        return new Uint8Array(value.buffer);
    }

    // no match
    return false;
}

// Actually inserts a key value into the selector document
// However, this checks there is no ambiguity in setting
// the value for the given key, throws otherwise
function insertIntoDocument(document, key, value) {
    Object.keys(document).forEach(existingKey => {
        if (
            (existingKey.length > key.length && existingKey.indexOf(`${key}.`) === 0) ||
            (key.length > existingKey.length && key.indexOf(`${existingKey}.`) === 0)
        ) {
            throw new Error(
                `cannot infer query fields to set, both paths '${existingKey}' and ` +
                `'${key}' are matched`
            );
        } else if (existingKey === key) {
            throw new Error(
                `cannot infer query fields to set, path '${key}' is matched twice`
            );
        }
    });

    document[key] = value;
}

// Returns a branched matcher that matches iff the given matcher does not.
// Note that this implicitly "deMorganizes" the wrapped function.  ie, it
// means that ALL branch values need to fail to match innerBranchedMatcher.
function invertBranchedMatcher(branchedMatcher) {
    return branchValues => {
        // We explicitly choose to strip arrayIndices here: it doesn't make sense to
        // say "update the array element that does not match something", at least
        // in mongo-land.
        return { result: !branchedMatcher(branchValues).result };
    };
}

export function isIndexable(obj) {
    return Array.isArray(obj) || _isPlainObject(obj);
}

export function isNumericKey(s) {
    return /^[0-9]+$/.test(s);
}

// Returns true if this is an object with at least one key and all keys begin
// with $.  Unless inconsistentOK is set, throws if some keys begin with $ and
// others don't.
export function isOperatorObject(valueSelector, inconsistentOK?) {
    if (!_isPlainObject(valueSelector)) {
        return false;
    }

    let theseAreOperators = undefined;
    Object.keys(valueSelector).forEach(selKey => {
        const thisIsOperator = selKey.substr(0, 1) === '$' || selKey === 'diff';

        if (theseAreOperators === undefined) {
            theseAreOperators = thisIsOperator;
        } else if (theseAreOperators !== thisIsOperator) {
            if (!inconsistentOK) {
                throw new Error(
                    `Inconsistent operator: ${JSON.stringify(valueSelector)}`
                );
            }

            theseAreOperators = false;
        }
    });

    return !!theseAreOperators; // {} has no operators
}

// Helper for $lt/$gt/$lte/$gte.
function makeInequality(cmpValueComparator) {
    return {
        compileElementSelector(operand) {
            // Arrays never compare false with non-arrays for any inequality.
            // XXX This was behavior we observed in pre-release MongoDB 2.5, but
            //     it seems to have been reverted.
            //     See https://jira.mongodb.org/browse/SERVER-11444
            if (Array.isArray(operand)) {
                return () => false;
            }

            // Special case: consider undefined and null the same (so true with
            // $gte/$lte).
            if (operand === undefined) {
                operand = null;
            }

            const operandType = _f._type(operand);

            return value => {
                if (value === undefined) {
                    value = null;
                }

                // Comparisons are never true among things of different type (except
                // null vs undefined).
                if (_f._type(value) !== operandType) {
                    return false;
                }

                return cmpValueComparator(_f._cmp(value, operand));
            };
        },
    };
}

// makeLookupFunction(key) returns a lookup function.
//
// A lookup function takes in a document and returns an array of matching
// branches.  If no arrays are found while looking up the key, this array will
// have exactly one branches (possibly 'undefined', if some segment of the key
// was not found).
//
// If arrays are found in the middle, this can have more than one element, since
// we 'branch'. When we 'branch', if there are more key segments to look up,
// then we only pursue branches that are plain objects (not arrays or scalars).
// This means we can actually end up with no branches!
//
// We do *NOT* branch on arrays that are found at the end (ie, at the last
// dotted member of the key). We just return that array; if you want to
// effectively 'branch' over the array's values, post-process the lookup
// function with expandArraysInBranches.
//
// Each branch is an object with keys:
//  - value: the value at the branch
//  - dontIterate: an optional bool; if true, it means that 'value' is an array
//    that expandArraysInBranches should NOT expand. This specifically happens
//    when there is a numeric index in the key, and ensures the
//    perhaps-surprising MongoDB behavior where {'a.0': 5} does NOT
//    match {a: [[5]]}.
//  - arrayIndices: if any array indexing was done during lookup (either due to
//    explicit numeric indices or implicit branching), this will be an array of
//    the array indices used, from outermost to innermost; it is falsey or
//    absent if no array index is used. If an explicit numeric index is used,
//    the index will be followed in arrayIndices by the string 'x'.
//
//    Note: arrayIndices is used for two purposes. First, it is used to
//    implement the '$' modifier feature, which only ever looks at its first
//    element.
//
//    Second, it is used for sort key generation, which needs to be able to tell
//    the difference between different paths. Moreover, it needs to
//    differentiate between explicit and implicit branching, which is why
//    there's the somewhat hacky 'x' entry: this means that explicit and
//    implicit array lookups will have different full arrayIndices paths. (That
//    code only requires that different paths have different arrayIndices; it
//    doesn't actually 'parse' arrayIndices. As an alternative, arrayIndices
//    could contain objects with flags like 'implicit', but I think that only
//    makes the code surrounding them more complex.)
//
//    (By the way, this field ends up getting passed around a lot without
//    cloning, so never mutate any arrayIndices field/var in this package!)
//
//
// At the top level, you may only pass in a plain object or array.
//
// See the test 'minimongo - lookup' for some examples of what lookup functions
// return.
export function makeLookupFunction(key, options: { forSort?: boolean } = {}) {
    const parts = key.split('.');
    const firstPart = parts.length ? parts[0] : '';
    const lookupRest = (
        parts.length > 1 &&
        makeLookupFunction(parts.slice(1).join('.'), options)
    );

    const omitUnnecessaryFields = result => {
        if (!result.dontIterate) {
            delete result.dontIterate;
        }

        if (result.arrayIndices && !result.arrayIndices.length) {
            delete result.arrayIndices;
        }

        return result;
    };

    // Doc will always be a plain object or an array.
    // apply an explicit numeric index, an array.
    return (doc, arrayIndices = []) => {
        if (Array.isArray(doc)) {
            // If we're being asked to do an invalid lookup into an array (non-integer
            // or out-of-bounds), return no results (which is different from returning
            // a single undefined result, in that `null` equality checks won't match).
            if (!(isNumericKey(firstPart) && firstPart < doc.length)) {
                return [];
            }

            // Remember that we used this array index. Include an 'x' to indicate that
            // the previous index came from being considered as an explicit array
            // index (not branching).
            arrayIndices = arrayIndices.concat(+firstPart, 'x');
        }

        // Do our first lookup.
        const firstLevel = doc[firstPart];

        // If there is no deeper to dig, return what we found.
        //
        // If what we found is an array, most value selectors will choose to treat
        // the elements of the array as matchable values in their own right, but
        // that's done outside of the lookup function. (Exceptions to this are $size
        // and stuff relating to $elemMatch.  eg, {a: {$size: 2}} does not match {a:
        // [[1, 2]]}.)
        //
        // That said, if we just did an *explicit* array lookup (on doc) to find
        // firstLevel, and firstLevel is an array too, we do NOT want value
        // selectors to iterate over it.  eg, {'a.0': 5} does not match {a: [[5]]}.
        // So in that case, we mark the return value as 'don't iterate'.
        if (!lookupRest) {
            return [omitUnnecessaryFields({
                arrayIndices,
                dontIterate: Array.isArray(doc) && Array.isArray(firstLevel),
                value: firstLevel
            })];
        }

        // We need to dig deeper.  But if we can't, because what we've found is not
        // an array or plain object, we're done. If we just did a numeric index into
        // an array, we return nothing here (this is a change in Mongo 2.5 from
        // Mongo 2.4, where {'a.0.b': null} stopped matching {a: [5]}). Otherwise,
        // return a single `undefined` (which can, for example, match via equality
        // with `null`).
        if (!isIndexable(firstLevel)) {
            if (Array.isArray(doc)) {
                return [];
            }

            return [omitUnnecessaryFields({ arrayIndices, value: undefined })];
        }

        const result = [];
        const appendToResult = more => {
            result.push(...more);
        };

        // Dig deeper: look up the rest of the parts on whatever we've found.
        // (lookupRest is smart enough to not try to do invalid lookups into
        // firstLevel if it's an array.)
        appendToResult(lookupRest(firstLevel, arrayIndices));

        // If we found an array, then in *addition* to potentially treating the next
        // part as a literal integer lookup, we should also 'branch': try to look up
        // the rest of the parts on each array element in parallel.
        //
        // In this case, we *only* dig deeper into array elements that are plain
        // objects. (Recall that we only got this far if we have further to dig.)
        // This makes sense: we certainly don't dig deeper into non-indexable
        // objects. And it would be weird to dig into an array: it's simpler to have
        // a rule that explicit integer indexes only apply to an outer array, not to
        // an array you find after a branching search.
        //
        // In the special case of a numeric part in a *sort selector* (not a query
        // selector), we skip the branching: we ONLY allow the numeric part to mean
        // 'look up this index' in that case, not 'also look up this index in all
        // the elements of the array'.
        if (Array.isArray(firstLevel) &&
            !(isNumericKey(parts[1]) && options.forSort)) {
            firstLevel.forEach((branch, arrayIndex) => {
                if (_isPlainObject(branch)) {
                    appendToResult(lookupRest(branch, arrayIndices.concat(arrayIndex)));
                }
            });
        }

        return result;
    };
}

type Minimongo_Error = Error & { setPropertyError?: boolean };
function MinimongoError(message, options: { field?: string } = {}) {
    if (typeof message === 'string' && options.field) {
        message += ` for field '${options.field}'`;
    }

    const error = new Error(message);
    error.name = 'MinimongoError';
    return error as Minimongo_Error;
}


export function nothingMatcher(docOrBranchedValues) {
    return { result: false };
}

// Takes an operator object (an object with $ keys) and returns a branched
// matcher for it.
function operatorBranchedMatcher(valueSelector, matcher, isRoot) {
    // Each valueSelector works separately on the various branches.  So one
    // operator can match one branch and another can match another branch.  This
    // is OK.
    const operatorMatchers = Object.keys(valueSelector).map(operator => {
        const operand = valueSelector[operator];

        const simpleRange = (
            ['$lt', '$lte', '$gt', '$gte'].includes(operator) &&
            typeof operand === 'number'
        );

        const simpleEquality = (
            ['$ne', '$eq'].includes(operator) &&
            operand !== Object(operand)
        );

        const simpleInclusion = (
            ['$in', '$nin'].includes(operator)
            && Array.isArray(operand)
            && !operand.some(x => x === Object(x))
        );

        if (!(simpleRange || simpleInclusion || simpleEquality)) {
            matcher._isSimple = false;
        }

        if (hasOwn.call(VALUE_OPERATORS, operator)) {
            return VALUE_OPERATORS[operator](operand, valueSelector, matcher, isRoot);
        }

        if (hasOwn.call(ELEMENT_OPERATORS, operator)) {
            const options = ELEMENT_OPERATORS[operator];
            return convertElementMatcherToBranchedMatcher(
                options.compileElementSelector(operand, valueSelector, matcher),
                options
            );
        }

        throw new Error(`Unrecognized operator: ${operator}`);
    });

    return andBranchedMatchers(operatorMatchers);
}

// paths - Array: list of mongo style paths
// newLeafFn - Function: of form function(path) should return a scalar value to
//                       put into list created for that path
// conflictFn - Function: of form function(node, path, fullPath) is called
//                        when building a tree path for 'fullPath' node on
//                        'path' was already a leaf with a value. Must return a
//                        conflict resolution.
// initial tree - Optional Object: starting tree.
// @returns - Object: tree represented as a set of nested objects
export function pathsToTree(paths, newLeafFn, conflictFn, root = {}) {
    paths.forEach(path => {
        const pathArray = path.split('.');
        let tree = root;

        // use .every just for iteration with break
        const success = pathArray.slice(0, -1).every((key, i) => {
            if (!hasOwn.call(tree, key)) {
                tree[key] = {};
            } else if (tree[key] !== Object(tree[key])) {
                tree[key] = conflictFn(
                    tree[key],
                    pathArray.slice(0, i + 1).join('.'),
                    path
                );

                // break out of loop if we are failing for this path
                if (tree[key] !== Object(tree[key])) {
                    return false;
                }
            }

            tree = tree[key];

            return true;
        });

        if (success) {
            const lastKey = pathArray[pathArray.length - 1];
            if (hasOwn.call(tree, lastKey)) {
                tree[lastKey] = conflictFn(tree[lastKey], path, path);
            } else {
                tree[lastKey] = newLeafFn(path);
            }
        }
    });

    return root;
}

// Makes sure we get 2 elements array and assume the first one to be x and
// the second one to y no matter what user passes.
// In case user passes { lon: x, lat: y } returns [x, y]
function pointToArray(point) {
    return Array.isArray(point) ? point.slice() : [point.x, point.y];
}

// Creating a document from an upsert is quite tricky.
// E.g. this selector: {"$or": [{"b.foo": {"$all": ["bar"]}}]}, should result
// in: {"b.foo": "bar"}
// But this selector: {"$or": [{"b": {"foo": {"$all": ["bar"]}}}]} should throw
// an error

// Some rules (found mainly with trial & error, so there might be more):
// - handle all childs of $and (or implicit $and)
// - handle $or nodes with exactly 1 child
// - ignore $or nodes with more than 1 child
// - ignore $nor and $not nodes
// - throw when a value can not be set unambiguously
// - every value for $all should be dealt with as separate $eq-s
// - threat all children of $all as $eq setters (=> set if $all.length === 1,
//   otherwise throw error)
// - you can not mix '$'-prefixed keys and non-'$'-prefixed keys
// - you can only have dotted keys on a root-level
// - you can not have '$'-prefixed keys more than one-level deep in an object

// Handles one key/value pair to put in the selector document
function populateDocumentWithKeyValue(document, key, value) {
    if (value && Object.getPrototypeOf(value) === Object.prototype) {
        populateDocumentWithObject(document, key, value);
    } else if (!(value instanceof RegExp)) {
        insertIntoDocument(document, key, value);
    }
}

// Handles a key, value pair to put in the selector document
// if the value is an object
function populateDocumentWithObject(document, key, value) {
    const keys = Object.keys(value);
    const unprefixedKeys = keys.filter(op => op[0] !== '$');

    if (unprefixedKeys.length > 0 || !keys.length) {
        // Literal (possibly empty) object ( or empty object )
        // Don't allow mixing '$'-prefixed with non-'$'-prefixed fields
        if (keys.length !== unprefixedKeys.length) {
            throw new Error(`unknown operator: ${unprefixedKeys[0]}`);
        }

        validateObject(value, key);
        insertIntoDocument(document, key, value);
    } else {
        Object.keys(value).forEach(op => {
            const object = value[op];

            if (op === '$eq') {
                populateDocumentWithKeyValue(document, key, object);
            } else if (op === '$all') {
                // every value for $all should be dealt with as separate $eq-s
                object.forEach(element =>
                    populateDocumentWithKeyValue(document, key, element)
                );
            }
        });
    }
}

// Fills a document with certain fields from an upsert selector
export function populateDocumentWithQueryFields(query, document = {}) {
    if (Object.getPrototypeOf(query) === Object.prototype) {
        // handle implicit $and
        Object.keys(query).forEach(key => {
            const value = query[key];

            if (key === '$and') {
                // handle explicit $and
                value.forEach(element =>
                    populateDocumentWithQueryFields(element, document)
                );
            } else if (key === '$or') {
                // handle $or nodes with exactly 1 child
                if (value.length === 1) {
                    populateDocumentWithQueryFields(value[0], document);
                }
            } else if (key[0] !== '$') {
                // Ignore other '$'-prefixed logical selectors
                populateDocumentWithKeyValue(document, key, value);
            }
        });
    }

    return document;
}

// Traverses the keys of passed projection and constructs a tree where all
// leaves are either all True or all False
// @returns Object:
//  - tree - Object - tree representation of keys involved in projection
//  (exception for '_id' as it is a special case handled separately)
//  - including - Boolean - "take only certain fields" type of projection
export function projectionDetails(fields) {
    // Find the non-_id keys (_id is handled specially because it is included
    // unless explicitly excluded). Sort the keys, so that our code to detect
    // overlaps like 'foo' and 'foo.bar' can assume that 'foo' comes first.
    let fieldsKeys = Object.keys(fields).sort();

    // If _id is the only field in the projection, do not remove it, since it is
    // required to determine if this is an exclusion or exclusion. Also keep an
    // inclusive _id, since inclusive _id follows the normal rules about mixing
    // inclusive and exclusive fields. If _id is not the only field in the
    // projection and is exclusive, remove it so it can be handled later by a
    // special case, since exclusive _id is always allowed.
    if (!(fieldsKeys.length === 1 && fieldsKeys[0] === '_id') &&
        !(fieldsKeys.includes('_id') && fields._id)) {
        fieldsKeys = fieldsKeys.filter(key => key !== '_id');
    }

    let including = null; // Unknown

    fieldsKeys.forEach(keyPath => {
        const rule = !!fields[keyPath];

        if (including === null) {
            including = rule;
        }

        // This error message is copied from MongoDB shell
        if (including !== rule) {
            throw MinimongoError(
                'You cannot currently mix including and excluding fields.'
            );
        }
    });

    const projectionRulesTree = pathsToTree(
        fieldsKeys,
        path => including,
        (node, path, fullPath) => {
            // Check passed projection fields' keys: If you have two rules such as
            // 'foo.bar' and 'foo.bar.baz', then the result becomes ambiguous. If
            // that happens, there is a probability you are doing something wrong,
            // framework should notify you about such mistake earlier on cursor
            // compilation step than later during runtime.  Note, that real mongo
            // doesn't do anything about it and the later rule appears in projection
            // project, more priority it takes.
            //
            // Example, assume following in mongo shell:
            // > db.coll.insert({ a: { b: 23, c: 44 } })
            // > db.coll.find({}, { 'a': 1, 'a.b': 1 })
            // {"_id": ObjectId("520bfe456024608e8ef24af3"), "a": {"b": 23}}
            // > db.coll.find({}, { 'a.b': 1, 'a': 1 })
            // {"_id": ObjectId("520bfe456024608e8ef24af3"), "a": {"b": 23, "c": 44}}
            //
            // Note, how second time the return set of keys is different.
            const currentPath = fullPath;
            const anotherPath = path;
            throw MinimongoError(
                `both ${currentPath} and ${anotherPath} found in fields option, ` +
                'using both of them may trigger unexpected behavior. Did you mean to ' +
                'use only one of them?'
            );
        });

    return { including, tree: projectionRulesTree };
}

// Takes a RegExp object and returns an element matcher.
export function regexpElementMatcher(regexp) {
    return value => {
        if (value instanceof RegExp) {
            return value.toString() === regexp.toString();
        }

        // Regexps only work against strings.
        if (typeof value !== 'string') {
            return false;
        }

        // Reset regexp's state to avoid inconsistent matching for objects with the
        // same value on consecutive calls of regexp.test. This happens only if the
        // regexp has the 'g' flag. Also note that ES6 introduces a new flag 'y' for
        // which we should *not* change the lastIndex but MongoDB doesn't support
        // either of these flags.
        regexp.lastIndex = 0;

        return regexp.test(value);
    };
}

// Validates the key in a path.
// Objects that are nested more then 1 level cannot have dotted fields
// or fields starting with '$'
function validateKeyInPath(key, path) {
    if (key.includes('.')) {
        throw new Error(
            `The dotted field '${key}' in '${path}.${key} is not valid for storage.`
        );
    }

    if (key[0] === '$') {
        throw new Error(
            `The dollar ($) prefixed field  '${path}.${key} is not valid for storage.`
        );
    }
}

// Recursively validates an object that is nested more than one level deep
function validateObject(object, path) {
    if (object && Object.getPrototypeOf(object) === Object.prototype) {
        Object.keys(object).forEach(key => {
            validateKeyInPath(key, path);
            validateObject(object[key], path + '.' + key);
        });
    }
}

function _isPlainObject(x) {
    return x && _f._type(x) === 3;
}

// helpers used by compiled selector code
export const _f = {
    // XXX for _all and _in, consider building 'inquery' at compile time..
    _type(v) {
        if (typeof v === 'number') {
            return 1;
        }

        if (typeof v === 'string') {
            return 2;
        }

        if (typeof v === 'boolean') {
            return 8;
        }

        if (Array.isArray(v)) {
            return 4;
        }

        if (v === null) {
            return 10;
        }

        // note that typeof(/x/) === "object"
        if (v instanceof RegExp) {
            return 11;
        }

        if (typeof v === 'function') {
            return 13;
        }

        if (v instanceof Date) {
            return 9;
        }

        if (isBinary(v)) {
            return 5;
        }

        if (v instanceof ObjectId) {
            return 7;
        }

        if (v instanceof Decimal128) {
            return 1;
        }

        // object
        return 3;

        // XXX support some/all of these:
        // 14, symbol
        // 15, javascript code with scope
        // 16, 18: 32-bit/64-bit integer
        // 17, timestamp
        // 255, minkey
        // 127, maxkey
    },

    // deep equality test: use for literal document and array matches
    _equal(a: any, b: any) {
        return equals(a, b, { keyOrderSensitive: true });
    },

    // maps a type code to a value that can be used to sort values of different
    // types
    _typeorder(t) {
        // http://www.mongodb.org/display/DOCS/What+is+the+Compare+Order+for+BSON+Types
        // XXX what is the correct sort position for Javascript code?
        // ('100' in the matrix below)
        // XXX minkey/maxkey
        return [
            -1,  // (not a type)
            1,   // number
            2,   // string
            3,   // object
            4,   // array
            5,   // binary
            -1,  // deprecated
            6,   // ObjectID
            7,   // bool
            8,   // Date
            0,   // null
            9,   // RegExp
            -1,  // deprecated
            100, // JS code
            2,   // deprecated (symbol)
            100, // JS code
            1,   // 32-bit int
            8,   // Mongo timestamp
            1    // 64-bit int
        ][t];
    },

    // compare two values of unknown type according to BSON ordering
    // semantics. (as an extension, consider 'undefined' to be less than
    // any other value.) return negative if a is less, positive if b is
    // less, or 0 if equal
    _cmp(a: any, b: any) {
        if (a === undefined) {
            return b === undefined ? 0 : -1;
        }

        if (b === undefined) {
            return 1;
        }

        let ta = _f._type(a);
        let tb = _f._type(b);

        const oa = _f._typeorder(ta);
        const ob = _f._typeorder(tb);

        if (oa !== ob) {
            return oa < ob ? -1 : 1;
        }

        // XXX need to implement this if we implement Symbol or integers, or
        // Timestamp
        if (ta !== tb) {
            throw Error('Missing type coercion logic in _cmp');
        }

        if (ta === 7) { // ObjectID
            // Convert to string.
            ta = tb = 2;
            a = a.toHexString();
            b = b.toHexString();
        }

        if (ta === 9) { // Date
            // Convert to millis.
            ta = tb = 1;
            a = isNaN(a) ? 0 : a.getTime();
            b = isNaN(b) ? 0 : b.getTime();
        }

        if (ta === 1) { // double
            if (a instanceof Decimal128) {
                return Decimal128.fromString((BigInt(a.toString()) - BigInt(b.toString())).toString());
            } else {
                return a - b;
            }
        }

        if (tb === 2) // string
            return a < b ? -1 : a === b ? 0 : 1;

        if (ta === 3) { // Object
            // this could be much more efficient in the expected case ...
            const toArray = object => {
                const result = [];

                Object.keys(object).forEach(key => {
                    result.push(key, object[key]);
                });

                return result;
            };

            return _f._cmp(toArray(a), toArray(b));
        }

        if (ta === 4) { // Array
            for (let i = 0; ; i++) {
                if (i === a.length) {
                    return i === b.length ? 0 : -1;
                }

                if (i === b.length) {
                    return 1;
                }

                const s = _f._cmp(a[i], b[i]);
                if (s !== 0) {
                    return s;
                }
            }
        }

        if (ta === 5) { // binary
            // Surprisingly, a small binary blob is always less than a large one in
            // Mongo.
            if (a.length !== b.length) {
                return a.length - b.length;
            }

            for (let i = 0; i < a.length; i++) {
                if (a[i] < b[i]) {
                    return -1;
                }

                if (a[i] > b[i]) {
                    return 1;
                }
            }

            return 0;
        }

        if (ta === 8) { // boolean
            if (a) {
                return b ? 0 : 1;
            }

            return b ? -1 : 0;
        }

        if (ta === 10) // null
            return 0;

        if (ta === 11) // regexp
            throw Error('Sorting not supported on regular expression'); // XXX

        // 13: javascript code
        // 14: symbol
        // 15: javascript code with scope
        // 16: 32-bit integer
        // 17: timestamp
        // 18: 64-bit integer
        // 255: minkey
        // 127: maxkey
        if (ta === 13) // javascript code
            throw Error('Sorting not supported on Javascript code'); // XXX

        throw Error('Unknown type to sort');
    },
};

export function _checkSupportedProjection(fields) {
    if (fields !== Object(fields) || Array.isArray(fields)) {
        throw MinimongoError('fields option must be an object');
    }

    Object.keys(fields).forEach(keyPath => {
        if (keyPath.split('.').includes('$')) {
            throw MinimongoError(
                'Minimongo doesn\'t support $ operator in projections yet.'
            );
        }

        const value = fields[keyPath];

        if (typeof value === 'object' &&
            ['$elemMatch', '$meta', '$slice'].some(key =>
                hasOwn.call(value, key)
            )) {
            throw MinimongoError(
                'Minimongo doesn\'t support operators in projections yet.'
            );
        }

        if (![1, 0, true, false].includes(value)) {
            throw MinimongoError(
                'Projection values should be one of 1, 0, true, or false'
            );
        }
    });
}

// XXX need a strategy for passing the binding of $ into this
// function, from the compiled selector
//
// maybe just {key.up.to.just.before.dollarsign: array_index}
//
// XXX atomicity: if one modification fails, do we roll back the whole
// change?
//
// options:
//   - isInsert is set when _modify is being called to compute the document to
//     insert as part of an upsert operation. We use this primarily to figure
//     out when to set the fields in $setOnInsert, if present.
export function _modify(doc, modifier, options: { isInsert?: boolean, arrayIndices?: number[] } = {}) {
    if (!_isPlainObject(modifier)) {
        throw MinimongoError('Modifier must be an object');
    }

    // Make sure the caller can't mutate our data structures.
    modifier = clone(modifier);

    const isModifier = isOperatorObject(modifier);
    const newDoc = isModifier ? clone(doc) : modifier;

    if (isModifier) {
        // apply modifiers to the doc.
        Object.keys(modifier).forEach(operator => {
            // Treat $setOnInsert as $set if this is an insert.
            const setOnInsert = options.isInsert && operator === '$setOnInsert';
            const modFunc = MODIFIERS[setOnInsert ? '$set' : operator];
            const operand = modifier[operator];

            if (!modFunc) {
                throw MinimongoError(`Invalid modifier specified ${operator}`);
            }

            Object.keys(operand).forEach(keypath => {
                const arg = operand[keypath];

                if (keypath === '') {
                    throw MinimongoError('An empty update path is not valid.');
                }

                const keyparts = keypath.split('.');

                if (!keyparts.every(Boolean)) {
                    throw MinimongoError(
                        `The update path '${keypath}' contains an empty field name, ` +
                        'which is not allowed.'
                    );
                }

                const target = findModTarget(newDoc, keyparts, {
                    arrayIndices: options.arrayIndices,
                    forbidArray: operator === '$rename',
                    noCreate: NO_CREATE_MODIFIERS[operator]
                });

                modFunc(target, keyparts.pop(), arg, keypath, newDoc);
            });
        });

        if (doc._id && !equals(doc._id, newDoc._id)) {
            throw MinimongoError(
                `After applying the update to the document {_id: "${doc._id}", ...},` +
                ' the (immutable) field \'_id\' was found to have been altered to ' +
                `_id: "${newDoc._id}"`
            );
        }
    } else {
        if (doc._id && modifier._id && !equals(doc._id, modifier._id)) {
            throw MinimongoError(
                `The _id field cannot be changed from {_id: "${doc._id}"} to ` +
                `{_id: "${modifier._id}"}`
            );
        }

        // replace the whole document
        assertHasValidFieldNames(modifier);
    }

    // move new document into place.
    Object.keys(doc).forEach(key => {
        // Note: this used to be for (var key in doc) however, this does not
        // work right in Opera. Deleting from a doc while iterating over it
        // would sometimes cause opera to skip some keys.
        if (key !== '_id') {
            delete doc[key];
        }
    });

    Object.keys(newDoc).forEach(key => {
        doc[key] = newDoc[key];
    });
};

// checks if all field names in an object are valid
function assertHasValidFieldNames(doc) {
    if (doc && typeof doc === 'object') {
        JSON.stringify(doc, (key, value) => {
            assertIsValidFieldName(key);
            return value;
        });
    }
}

const MODIFIERS = {
    $currentDate(target, field, arg) {
        if (typeof arg === 'object' && hasOwn.call(arg, '$type')) {
            if (arg.$type !== 'date') {
                throw MinimongoError(
                    'Minimongo does currently only support the date type in ' +
                    '$currentDate modifiers',
                    { field }
                );
            }
        } else if (arg !== true) {
            throw MinimongoError('Invalid $currentDate modifier', { field });
        }

        target[field] = new Date();
    },
    $min(target, field, arg) {
        if (typeof arg !== 'number') {
            throw MinimongoError('Modifier $min allowed for numbers only', { field });
        }

        if (field in target) {
            if (typeof target[field] !== 'number') {
                throw MinimongoError(
                    'Cannot apply $min modifier to non-number',
                    { field }
                );
            }

            if (target[field] > arg) {
                target[field] = arg;
            }
        } else {
            target[field] = arg;
        }
    },
    $max(target, field, arg) {
        if (typeof arg !== 'number') {
            throw MinimongoError('Modifier $max allowed for numbers only', { field });
        }

        if (field in target) {
            if (typeof target[field] !== 'number') {
                throw MinimongoError(
                    'Cannot apply $max modifier to non-number',
                    { field }
                );
            }

            if (target[field] < arg) {
                target[field] = arg;
            }
        } else {
            target[field] = arg;
        }
    },
    $inc(target, field, arg) {
        if (typeof arg !== 'number') {
            throw MinimongoError('Modifier $inc allowed for numbers only', { field });
        }

        if (field in target) {
            if (typeof target[field] !== 'number') {
                throw MinimongoError(
                    'Cannot apply $inc modifier to non-number',
                    { field }
                );
            }

            target[field] += arg;
        } else {
            target[field] = arg;
        }
    },
    $set(target, field, arg) {
        if (target !== Object(target)) { // not an array or an object
            const error = MinimongoError(
                'Cannot set property on non-object field',
                { field }
            );
            error.setPropertyError = true;
            throw error;
        }

        if (target === null) {
            const error = MinimongoError('Cannot set property on null', { field });
            error.setPropertyError = true;
            throw error;
        }

        assertHasValidFieldNames(arg);

        target[field] = arg;
    },
    $setOnInsert(target, field, arg) {
        // converted to `$set` in `_modify`
    },
    $unset(target, field, arg) {
        if (target !== undefined) {
            if (target instanceof Array) {
                if (field in target) {
                    target[field] = null;
                }
            } else {
                delete target[field];
            }
        }
    },
    $push(target, field, arg) {
        if (target[field] === undefined) {
            target[field] = [];
        }

        if (!(target[field] instanceof Array)) {
            throw MinimongoError('Cannot apply $push modifier to non-array', { field });
        }

        if (!(arg && arg.$each)) {
            // Simple mode: not $each
            assertHasValidFieldNames(arg);

            target[field].push(arg);

            return;
        }

        // Fancy mode: $each (and maybe $slice and $sort and $position)
        const toPush = arg.$each;
        if (!(toPush instanceof Array)) {
            throw MinimongoError('$each must be an array', { field });
        }

        assertHasValidFieldNames(toPush);

        // Parse $position
        let position = undefined;
        if ('$position' in arg) {
            if (typeof arg.$position !== 'number') {
                throw MinimongoError('$position must be a numeric value', { field });
            }

            // XXX should check to make sure integer
            if (arg.$position < 0) {
                throw MinimongoError(
                    '$position in $push must be zero or positive',
                    { field }
                );
            }

            position = arg.$position;
        }

        // Parse $slice.
        let slice = undefined;
        if ('$slice' in arg) {
            if (typeof arg.$slice !== 'number') {
                throw MinimongoError('$slice must be a numeric value', { field });
            }

            // XXX should check to make sure integer
            slice = arg.$slice;
        }

        // Parse $sort.
        let sortFunction = undefined;
        if (arg.$sort) {
            if (slice === undefined) {
                throw MinimongoError('$sort requires $slice to be present', { field });
            }

            // XXX this allows us to use a $sort whose value is an array, but that's
            // actually an extension of the Node driver, so it won't work
            // server-side. Could be confusing!
            // XXX is it correct that we don't do geo-stuff here?
            sortFunction = new MinimongoSorter(arg.$sort).getComparator();

            toPush.forEach(element => {
                if (_f._type(element) !== 3) {
                    throw MinimongoError(
                        '$push like modifiers using $sort require all elements to be ' +
                        'objects',
                        { field }
                    );
                }
            });
        }

        // Actually push.
        if (position === undefined) {
            toPush.forEach(element => {
                target[field].push(element);
            });
        } else {
            target[field].splice(position, 0, ...toPush);
        }

        // Actually sort.
        if (sortFunction) {
            target[field].sort(sortFunction);
        }

        // Actually slice.
        if (slice !== undefined) {
            if (slice === 0) {
                target[field] = []; // differs from Array.slice!
            } else if (slice < 0) {
                target[field] = target[field].slice(slice);
            } else {
                target[field] = target[field].slice(0, slice);
            }
        }
    },
    $pushAll(target, field, arg) {
        if (!(typeof arg === 'object' && arg instanceof Array)) {
            throw MinimongoError('Modifier $pushAll/pullAll allowed for arrays only');
        }

        assertHasValidFieldNames(arg);

        const toPush = target[field];

        if (toPush === undefined) {
            target[field] = arg;
        } else if (!(toPush instanceof Array)) {
            throw MinimongoError(
                'Cannot apply $pushAll modifier to non-array',
                { field }
            );
        } else {
            toPush.push(...arg);
        }
    },
    $addToSet(target, field, arg) {
        let isEach = false;

        if (typeof arg === 'object') {
            // check if first key is '$each'
            const keys = Object.keys(arg);
            if (keys[0] === '$each') {
                isEach = true;
            }
        }

        const values = isEach ? arg.$each : [arg];

        assertHasValidFieldNames(values);

        const toAdd = target[field];
        if (toAdd === undefined) {
            target[field] = values;
        } else if (!(toAdd instanceof Array)) {
            throw MinimongoError(
                'Cannot apply $addToSet modifier to non-array',
                { field }
            );
        } else {
            values.forEach(value => {
                if (toAdd.some(element => _f._equal(value, element))) {
                    return;
                }

                toAdd.push(value);
            });
        }
    },
    $pop(target, field, arg) {
        if (target === undefined) {
            return;
        }

        const toPop = target[field];

        if (toPop === undefined) {
            return;
        }

        if (!(toPop instanceof Array)) {
            throw MinimongoError('Cannot apply $pop modifier to non-array', { field });
        }

        if (typeof arg === 'number' && arg < 0) {
            toPop.splice(0, 1);
        } else {
            toPop.pop();
        }
    },
    $pull(target, field, arg) {
        if (target === undefined) {
            return;
        }

        const toPull = target[field];
        if (toPull === undefined) {
            return;
        }

        if (!(toPull instanceof Array)) {
            throw MinimongoError(
                'Cannot apply $pull/pullAll modifier to non-array',
                { field }
            );
        }

        let out;
        if (arg != null && typeof arg === 'object' && !(arg instanceof Array)) {
            // XXX would be much nicer to compile this once, rather than
            // for each document we modify.. but usually we're not
            // modifying that many documents, so we'll let it slide for
            // now

            // XXX Minimongo.Matcher isn't up for the job, because we need
            // to permit stuff like {$pull: {a: {$gt: 4}}}.. something
            // like {$gt: 4} is not normally a complete selector.
            // same issue as $elemMatch possibly?
            const matcher = new MinimongoMatcher(arg);

            out = toPull.filter(element => !matcher.documentMatches(element).result);
        } else {
            out = toPull.filter(element => !_f._equal(element, arg));
        }

        target[field] = out;
    },
    $pullAll(target, field, arg) {
        if (!(typeof arg === 'object' && arg instanceof Array)) {
            throw MinimongoError(
                'Modifier $pushAll/pullAll allowed for arrays only',
                { field }
            );
        }

        if (target === undefined) {
            return;
        }

        const toPull = target[field];

        if (toPull === undefined) {
            return;
        }

        if (!(toPull instanceof Array)) {
            throw MinimongoError(
                'Cannot apply $pull/pullAll modifier to non-array',
                { field }
            );
        }

        target[field] = toPull.filter(object =>
            !arg.some(element => _f._equal(object, element))
        );
    },
    $rename(target, field, arg, keypath, doc) {
        // no idea why mongo has this restriction..
        if (keypath === arg) {
            throw MinimongoError('$rename source must differ from target', { field });
        }

        if (target === null) {
            throw MinimongoError('$rename source field invalid', { field });
        }

        if (typeof arg !== 'string') {
            throw MinimongoError('$rename target must be a string', { field });
        }

        if (arg.includes('\0')) {
            // Null bytes are not allowed in Mongo field names
            // https://docs.mongodb.com/manual/reference/limits/#Restrictions-on-Field-Names
            throw MinimongoError(
                'The \'to\' field for $rename cannot contain an embedded null byte',
                { field }
            );
        }

        if (target === undefined) {
            return;
        }

        const object = target[field];

        delete target[field];

        const keyparts = arg.split('.');
        const target2 = findModTarget(doc, keyparts, { forbidArray: true });

        if (target2 === null) {
            throw MinimongoError('$rename target field invalid', { field });
        }

        target2[keyparts.pop()] = object;
    },
    $bit(target, field, arg) {
        // XXX mongo only supports $bit on integers, and we only support
        // native javascript numbers (doubles) so far, so we can't support $bit
        throw MinimongoError('$bit is not supported', { field });
    },
    $v() {
        // As discussed in https://github.com/meteor/meteor/issues/9623,
        // the `$v` operator is not needed by Meteor, but problems can occur if
        // it's not at least callable (as of Mongo >= 3.6). It's defined here as
        // a no-op to work around these problems.
    }
};

const NO_CREATE_MODIFIERS = {
    $pop: true,
    $pull: true,
    $pullAll: true,
    $rename: true,
    $unset: true
};

const invalidCharMsg = {
    $: 'start with \'$\'',
    '.': 'contain \'.\'',
    '\0': 'contain null bytes'
};

function assertIsValidFieldName(key) {
    let match;
    if (typeof key === 'string' && (match = key.match(/^\$|\.|\0/))) {
        throw MinimongoError(`Key ${key} must not ${invalidCharMsg[match[0]]}`);
    }
}

// for a.b.c.2.d.e, keyparts should be ['a', 'b', 'c', '2', 'd', 'e'],
// and then you would operate on the 'e' property of the returned
// object.
//
// if options.noCreate is falsey, creates intermediate levels of
// structure as necessary, like mkdir -p (and raises an exception if
// that would mean giving a non-numeric property to an array.) if
// options.noCreate is true, return undefined instead.
//
// may modify the last element of keyparts to signal to the caller that it needs
// to use a different value to index into the returned object (for example,
// ['a', '01'] -> ['a', 1]).
//
// if forbidArray is true, return null if the keypath goes through an array.
//
// if options.arrayIndices is set, use its first element for the (first) '$' in
// the path.
function findModTarget(doc, keyparts, options: { forbidArray?: boolean, arrayIndices?: any[], noCreate?: boolean } = {}) {
    let usedArrayIndex = false;

    for (let i = 0; i < keyparts.length; i++) {
        const last = i === keyparts.length - 1;
        let keypart = keyparts[i];

        if (!isIndexable(doc)) {
            if (options.noCreate) {
                return undefined;
            }

            const error = MinimongoError(
                `cannot use the part '${keypart}' to traverse ${doc}`
            );
            error.setPropertyError = true;
            throw error;
        }

        if (doc instanceof Array) {
            if (options.forbidArray) {
                return null;
            }

            if (keypart === '$') {
                if (usedArrayIndex) {
                    throw MinimongoError('Too many positional (i.e. \'$\') elements');
                }

                if (!options.arrayIndices || !options.arrayIndices.length) {
                    throw MinimongoError(
                        'The positional operator did not find the match needed from the ' +
                        'query'
                    );
                }

                keypart = options.arrayIndices[0];
                usedArrayIndex = true;
            } else if (isNumericKey(keypart)) {
                keypart = parseInt(keypart);
            } else {
                if (options.noCreate) {
                    return undefined;
                }

                throw MinimongoError(
                    `can't append to array using string field name [${keypart}]`
                );
            }

            if (last) {
                keyparts[i] = keypart; // handle 'a.01'
            }

            if (options.noCreate && keypart >= doc.length) {
                return undefined;
            }

            while (doc.length < keypart) {
                doc.push(null);
            }

            if (!last) {
                if (doc.length === keypart) {
                    doc.push({});
                } else if (typeof doc[keypart] !== 'object') {
                    throw MinimongoError(
                        `can't modify field '${keyparts[i + 1]}' of list value ` +
                        JSON.stringify(doc[keypart])
                    );
                }
            }
        } else {
            assertIsValidFieldName(keypart);

            if (!(keypart in doc)) {
                if (options.noCreate) {
                    return undefined;
                }

                if (!last) {
                    doc[keypart] = {};
                }
            }
        }

        if (last) {
            return doc;
        }

        doc = doc[keypart];
    }

    // notreached
}

export function combineImportantPathsIntoProjection(paths, projection) {
    const details = projectionDetails(projection);

    // merge the paths to include
    const tree = pathsToTree(
        paths,
        path => true,
        (node, path, fullPath) => true,
        details.tree
    );
    const mergedProjection = treeToPaths(tree);

    if (details.including) {
        // both selector and projection are pointing on fields to include
        // so we can just return the merged tree
        return mergedProjection;
    }

    // selector is pointing at fields to include
    // projection is pointing at fields to exclude
    // make sure we don't exclude important paths
    const mergedExclProjection = {};

    Object.keys(mergedProjection).forEach(path => {
        if (!mergedProjection[path]) {
            mergedExclProjection[path] = false;
        }
    });

    return mergedExclProjection;
}

// Returns a set of key paths similar to
// { 'foo.bar': 1, 'a.b.c': 1 }
function treeToPaths(tree, prefix = '') {
    const result = {};

    Object.keys(tree).forEach(key => {
        const value = tree[key];
        if (value === Object(value)) {
            Object.assign(result, treeToPaths(value, `${prefix + key}.`));
        } else {
            result[prefix + key] = value;
        }
    });

    return result;
}

export function _pathsElidingNumericKeys(paths) {
    return paths.map(path =>
        path.split('.').filter(part => !isNumericKey(part)).join('.')
    );
}


// Knows how to compile a fields projection to a predicate function.
// @returns - Function: a closure that filters out an object according to the
//            fields projection rules:
//            @param obj - Object: MongoDB-styled document
//            @returns - Object: a document with the fields filtered out
//                       according to projection rules. Doesn't retain subfields
//                       of passed argument.
export function _compileProjection(fields) {
    _checkSupportedProjection(fields);

    const _idProjection = fields._id === undefined ? true : fields._id;
    const details = projectionDetails(fields);

    // returns transformed doc according to ruleTree
    const transform = (doc, ruleTree) => {
        // Special case for "sets"
        if (Array.isArray(doc)) {
            return doc.map(subdoc => transform(subdoc, ruleTree));
        }

        const result = details.including ? {} : clone(doc);

        Object.keys(ruleTree).forEach(key => {
            if (doc == null || !hasOwn.call(doc, key)) {
                return;
            }

            const rule = ruleTree[key];

            if (rule === Object(rule)) {
                // For sub-objects/subsets we branch
                if (doc[key] === Object(doc[key])) {
                    result[key] = transform(doc[key], rule);
                }
            } else if (details.including) {
                // Otherwise we don't even touch this subfield
                result[key] = clone(doc[key]);
            } else {
                delete result[key];
            }
        });

        return doc != null ? result : doc;
    };

    return doc => {
        const result = transform(doc, details.tree);

        if (_idProjection && hasOwn.call(doc, '_id')) {
            result._id = doc._id;
        }

        if (!_idProjection && hasOwn.call(result, '_id')) {
            delete result._id;
        }

        return result;
    };
}
