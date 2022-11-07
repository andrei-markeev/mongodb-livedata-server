import { clone, equals } from "../ejson/ejson";

interface PrecedenceItem {
    subscriptionHandle: any;
    value: any;
}

// Represents a single document in a SessionCollectionView
export class SessionDocumentView {
    public existsIn: Set<any>; // set of subscriptionHandle
    private dataByKey: Map<string, PrecedenceItem[]>; // key-> [ {subscriptionHandle, value} by precedence]
    constructor() {
        this.existsIn = new Set();
        this.dataByKey = new Map();
    };

    getFields() {
        var ret: Record<string, any> = {};
        this.dataByKey.forEach((precedenceList, key) => {
            ret[key] = precedenceList[0].value;
        });
        return ret;
    }

    clearField(subscriptionHandle, key: string, changeCollector: Object) {
        // Publish API ignores _id if present in fields
        if (key === "_id")
            return;
        const precedenceList = this.dataByKey.get(key);

        // It's okay to clear fields that didn't exist. No need to throw
        // an error.
        if (!precedenceList)
            return;

        let removedValue = undefined;
        for (var i = 0; i < precedenceList.length; i++) {
            var precedence = precedenceList[i];
            if (precedence.subscriptionHandle === subscriptionHandle) {
                // The view's value can only change if this subscription is the one that
                // used to have precedence.
                if (i === 0)
                    removedValue = precedence.value;
                precedenceList.splice(i, 1);
                break;
            }
        }
        if (precedenceList.length === 0) {
            this.dataByKey.delete(key);
            changeCollector[key] = undefined;
        } else if (removedValue !== undefined && !equals(removedValue, precedenceList[0].value)) {
            changeCollector[key] = precedenceList[0].value;
        }
    }

    changeField(subscriptionHandle, key: string, value: any, changeCollector: Object, isAdd: boolean) {
        // Publish API ignores _id if present in fields
        if (key === "_id")
            return;

        // Don't share state with the data passed in by the user.
        value = clone(value);

        if (!this.dataByKey.has(key)) {
            this.dataByKey.set(key, [{
                subscriptionHandle: subscriptionHandle,
                value: value
            }]);
            changeCollector[key] = value;
            return;
        }
        const precedenceList = this.dataByKey.get(key);
        let elt;
        if (!isAdd) {
            elt = precedenceList.find(precedence => {
                return precedence.subscriptionHandle === subscriptionHandle;
            });
        }

        if (elt) {
            if (elt === precedenceList[0] && !equals(value, elt.value)) {
                // this subscription is changing the value of this field.
                changeCollector[key] = value;
            }
            elt.value = value;
        } else {
            // this subscription is newly caring about this field
            precedenceList.push({ subscriptionHandle: subscriptionHandle, value: value });
        }

    }
}
