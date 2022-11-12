const hasOwn = Object.prototype.hasOwnProperty;

export class Hook {
    private nextCallbackId: number;
    private callbacks: Record<string, Function>;
    private exceptionHandler: Function;

    constructor(options?) {
        options = options || {};
        this.nextCallbackId = 0;
        this.callbacks = Object.create(null);
        // Whether to wrap callbacks with Meteor.bindEnvironment

        if (options.exceptionHandler) {
            this.exceptionHandler = options.exceptionHandler;
        } else if (options.debugPrintExceptions) {
            if (typeof options.debugPrintExceptions !== "string") {
                throw new Error("Hook option debugPrintExceptions should be a string");
            }
            this.exceptionHandler = options.debugPrintExceptions;
        }
    }

    register(callback) {
        var exceptionHandler = this.exceptionHandler || function (exception) {
            // Note: this relies on the undocumented fact that if bindEnvironment's
            // onException throws, and you are invoking the callback either in the
            // browser or from within a Fiber in Node, the exception is propagated.
            throw exception;
        };

        callback = dontBindEnvironment(callback, exceptionHandler);

        var id = this.nextCallbackId++;
        this.callbacks[id] = callback;

        return {
            callback,
            stop: () => {
                delete this.callbacks[id];
            }
        };
    }

    // For each registered callback, call the passed iterator function
    // with the callback.
    //
    // The iterator function can choose whether or not to call the
    // callback.  (For example, it might not call the callback if the
    // observed object has been closed or terminated).
    //
    // The iteration is stopped if the iterator function returns a falsy
    // value or throws an exception.
    each(iterator: (callback: Function) => boolean) {
        var ids = Object.keys(this.callbacks);
        for (var i = 0; i < ids.length; ++i) {
            var id = ids[i];
            // check to see if the callback was removed during iteration
            if (hasOwn.call(this.callbacks, id)) {
                var callback = this.callbacks[id];
                if (!iterator(callback)) {
                    break;
                }
            }
        }
    }
}

function dontBindEnvironment(func, onException, _this?) {
    if (!onException || typeof (onException) === 'string') {
        var description = onException || "callback of async function";
        onException = function (error) {
            console.error("Exception in " + description, error);
        };
    }

    return function (...args) {
        try {
            var ret = func.apply(_this, args);
        } catch (e) {
            onException(e);
        }
        return ret;
    };
}