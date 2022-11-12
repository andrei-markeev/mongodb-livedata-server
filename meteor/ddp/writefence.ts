// A write fence collects a group of writes, and provides a callback
// when all of the writes are fully committed and propagated (all

import { OplogObserveDriver } from "../mongo/oplog-observe-driver";

// observers have been notified of the write and acknowledged it.)
export class _WriteFence {

    public fired: boolean;
    public _oplogObserveDrivers: Record<string, OplogObserveDriver>;

    private armed: boolean;
    private retired: boolean;
    private outstanding_writes: number;
    private before_fire_callbacks: Function[];
    private completion_callbacks: Function[];

    constructor () {
        this.armed = false;
        this.fired = false;
        this.retired = false;
        this.outstanding_writes = 0;
        this.before_fire_callbacks = [];
        this.completion_callbacks = [];
    };

    // The current write fence. When there is a current write fence, code
    // that writes to databases should register their writes with it using
    // beginWrite().
    //
    public static _CurrentWriteFence = new _WriteFence();

    // Start tracking a write, and return an object to represent it. The
    // object has a single method, committed(). This method should be
    // called when the write is fully committed and propagated. You can
    // continue to add writes to the WriteFence up until it is triggered
    // (calls its callbacks because all writes have committed.)
    beginWrite() {
        var self = this;

        if (self.retired)
            return { committed: function () { } };

        if (self.fired)
            throw new Error("fence has already activated -- too late to add writes");

        self.outstanding_writes++;
        var committed = false;
        return {
            committed: function () {
                if (committed)
                    throw new Error("committed called twice on the same write");
                committed = true;
                self.outstanding_writes--;
                self._maybeFire();
            }
        };
    }

    // Arm the fence. Once the fence is armed, and there are no more
    // uncommitted writes, it will activate.
    arm() {
        var self = this;
        if (self === _WriteFence._CurrentWriteFence)
            throw Error("Can't arm the current fence");
        self.armed = true;
        self._maybeFire();
    }

    // Register a function to be called once before firing the fence.
    // Callback function can add new writes to the fence, in which case
    // it won't fire until those writes are done as well.
    onBeforeFire(func) {
        var self = this;
        if (self.fired)
            throw new Error("fence has already activated -- too late to " +
                "add a callback");
        self.before_fire_callbacks.push(func);
    }

    // Register a function to be called when the fence fires.
    onAllCommitted(func) {
        var self = this;
        if (self.fired)
            throw new Error("fence has already activated -- too late to " +
                "add a callback");
        self.completion_callbacks.push(func);
    }

    _maybeFire() {
        var self = this;
        if (self.fired)
            throw new Error("write fence already activated?");
        if (self.armed && !self.outstanding_writes) {
            self.outstanding_writes++;
            while (self.before_fire_callbacks.length > 0) {
                var callbacks = self.before_fire_callbacks;
                self.before_fire_callbacks = [];
                for (const callback of callbacks)
                    invokeCallback(callback, self);
            }
            self.outstanding_writes--;

            if (!self.outstanding_writes) {
                self.fired = true;
                var callbacks = self.completion_callbacks;
                self.completion_callbacks = [];
                for (const callback of callbacks)
                    invokeCallback(callback, self);
            }
        }
    }

    // Deactivate this fence so that adding more writes has no effect.
    // The fence must have already fired.
    retire() {
        var self = this;
        if (!self.fired)
            throw new Error("Can't retire a fence that hasn't fired.");
        self.retired = true;
    }
}

function invokeCallback(func: Function, self: _WriteFence) {
    try {
        func(self);
    } catch (err) {
        console.error("exception in write fence callback", err);
    }
}
