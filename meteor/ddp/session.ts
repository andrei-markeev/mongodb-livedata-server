import { MethodInvocation } from "./method-invocation";
import { _WriteFence } from "./writefence";
import DoubleEndedQueue from "double-ended-queue";
import { Random } from "../random/main";
import { StreamServerSocket } from "./stream_server";
import { DDP, ddpError, DDPServer, wrapInternalException } from "./livedata_server";
import { Heartbeat } from "./heartbeat";
import { stringifyDDP } from "./utils";
import { DiffSequence } from "../diff-sequence/diff";
import { SessionCollectionView } from "./session-collection-view";
import { Subscription, SubscriptionHandle } from "./subscription";
import { OrderedDict } from "../ordered-dict/ordered_dict";

export interface SessionConnectionHandle {
    id: string;
    close: Function;
    onClose: Function;
    clientAddress: any;
    httpHeaders: Record<string, any>
}

interface DDPMessage {
    msg: string;
    id: string;
    version?: string;
    name?: string;
    method?: string;
    params?: any[]
    randomSeed?: any;
}

export class DDPSession {
    public id: string;
    public server: DDPServer;
    public inQueue: DoubleEndedQueue<any>;
    public userId: string | null;
    public connectionHandle: SessionConnectionHandle;
    public _dontStartNewUniversalSubs: boolean;
    public _socketUrl: string;
    public version: string;

    private socket: StreamServerSocket;
    private initialized: boolean;
    private workerRunning: boolean;
    private _namedSubs: Map<string, Subscription>;
    private _universalSubs: any[];
    private collectionViews: Map<string, SessionCollectionView>;
    private _isSending: boolean;
    private _pendingReady: string[];
    private _closeCallbacks: Function[];
    private _respondToPings: boolean;
    private heartbeat: Heartbeat;

    constructor(server: DDPServer, version: string, socket: StreamServerSocket, options) {
        var self = this;
        self.id = Random.id();

        self.server = server;
        self.version = version;

        self.initialized = false;
        self.socket = socket;

        // Set to null when the session is destroyed. Multiple places below
        // use this to determine if the session is alive or not.
        self.inQueue = new DoubleEndedQueue();

        self.workerRunning = false;

        // Sub objects for active subscriptions
        self._namedSubs = new Map();
        self._universalSubs = [];

        self.userId = null;

        self.collectionViews = new Map();

        // Set this to false to not send messages when collectionViews are
        // modified. This is done when rerunning subs in _setUserId and those messages
        // are calculated via a diff instead.
        self._isSending = true;

        // If this is true, don't start a newly-created universal publisher on this
        // session. The session will take care of starting it when appropriate.
        self._dontStartNewUniversalSubs = false;

        // When we are rerunning subscriptions, any ready messages
        // we want to buffer up for when we are done rerunning subscriptions
        self._pendingReady = [];

        // List of callbacks to call when this connection is closed.
        self._closeCallbacks = [];


        // XXX HACK: If a sockjs connection, save off the URL. This is
        // temporary and will go away in the near future.
        self._socketUrl = socket.url;

        // Allow tests to disable responding to pings.
        self._respondToPings = options.respondToPings;

        // This object is the public interface to the session. In the public
        // API, it is called the `connection` object.  Internally we call it
        // a `connectionHandle` to avoid ambiguity.
        self.connectionHandle = {
            id: self.id,
            close: function () {
                self.close();
            },
            onClose: function (cb) {
                if (self.inQueue) {
                    self._closeCallbacks.push(cb);
                } else {
                    setImmediate(cb);
                }
            },
            clientAddress: self._clientAddress(),
            httpHeaders: self.socket.headers
        };

        self.send({ msg: 'connected', session: self.id });

        // On initial connect, spin up all the universal publishers.
        setImmediate(() => this.startUniversalSubs());

        if (version !== 'pre1' && options.heartbeatInterval !== 0) {
            // We no longer need the low level timeout because we have heartbeats.
            socket.setWebsocketTimeout(0);

            self.heartbeat = new Heartbeat({
                heartbeatInterval: options.heartbeatInterval,
                heartbeatTimeout: options.heartbeatTimeout,
                onTimeout: function () {
                    self.close();
                },
                sendPing: function () {
                    self.send({ msg: 'ping' });
                }
            });
            self.heartbeat.start();
        }

    }

    sendReady(subscriptionIds: string[]) {
        var self = this;
        if (self._isSending)
            self.send({ msg: "ready", subs: subscriptionIds });
        else {
            for (const subscriptionId of subscriptionIds) {
                self._pendingReady.push(subscriptionId);
            }
        }
    }

    _canSend(collectionName) {
        return this._isSending || !this.server.getPublicationStrategy(collectionName).useCollectionView;
    }

    sendInitialAdds(collectionName: string, docs: Map<string, any> | OrderedDict) {
        if (this._canSend(collectionName)) {
            const items = [];
            docs.forEach(doc => items.push(doc));
            this.send({ msg: "init", collection: collectionName, items });
        }
    }

    sendAdded(collectionName: string, id: string, fields: Record<string, any>) {
        if (this._canSend(collectionName))
            this.send({ msg: "added", collection: collectionName, id, fields });
    }

    sendChanged(collectionName: string, id: string, fields: Record<string, any>) {
        if (fields == null || Object.keys(fields).length === 0)
            return;

        if (this._canSend(collectionName)) {
            this.send({
                msg: "changed",
                collection: collectionName,
                id,
                fields
            });
        }
    }

    sendRemoved(collectionName: string, id: string) {
        if (this._canSend(collectionName))
            this.send({ msg: "removed", collection: collectionName, id });
    }

    getSendCallbacks() {
        return {
            added: this.sendAdded.bind(this),
            changed: this.sendChanged.bind(this),
            removed: this.sendRemoved.bind(this)
        };
    }

    getCollectionView(collectionName: string) {
        var self = this;
        var ret = self.collectionViews.get(collectionName);
        if (!ret) {
            ret = new SessionCollectionView(collectionName, self.getSendCallbacks());
            self.collectionViews.set(collectionName, ret);
        }
        return ret;
    }

    initialAdds(subscriptionHandle: SubscriptionHandle, collectionName: string, docs: Map<string, any> | OrderedDict) {
        if (this.server.getPublicationStrategy(collectionName).useCollectionView) {
            const view = this.getCollectionView(collectionName);
            docs.forEach((doc, id) => view.added(subscriptionHandle, id, doc));
        } else {
            this.sendInitialAdds(collectionName, docs);
        }
    }

    added(subscriptionHandle: SubscriptionHandle, collectionName: string, id: string, fields: Record<string, any>) {
        if (this.server.getPublicationStrategy(collectionName).useCollectionView) {
            const view = this.getCollectionView(collectionName);
            view.added(subscriptionHandle, id, fields);
        } else {
            this.sendAdded(collectionName, id, fields);
        }
    }

    removed(subscriptionHandle: SubscriptionHandle, collectionName: string, id: string) {
        if (this.server.getPublicationStrategy(collectionName).useCollectionView) {
            const view = this.getCollectionView(collectionName);
            view.removed(subscriptionHandle, id);
            if (view.isEmpty()) {
                this.collectionViews.delete(collectionName);
            }
        } else {
            this.sendRemoved(collectionName, id);
        }
    }

    changed(subscriptionHandle: SubscriptionHandle, collectionName: string, id: string, fields: Record<string, any>) {
        if (this.server.getPublicationStrategy(collectionName).useCollectionView) {
            const view = this.getCollectionView(collectionName);
            view.changed(subscriptionHandle, id, fields);
        } else {
            this.sendChanged(collectionName, id, fields);
        }
    }

    startUniversalSubs() {
        // Make a shallow copy of the set of universal handlers and start them. If
        // additional universal publishers start while we're running them (due to
        // yielding), they will run separately as part of Server.publish.
        var handlers = [ ...this.server.universal_publish_handlers ];
        for (const handler of handlers) {
            this._startSubscription(handler);
        }
    }

    // Destroy this session and unregister it at the server.
    close() {
        var self = this;

        // Destroy this session, even if it's not registered at the
        // server. Stop all processing and tear everything down. If a socket
        // was attached, close it.

        // Already destroyed.
        if (!self.inQueue)
            return;

        // Drop the merge box data immediately.
        self.inQueue = null;
        self.collectionViews = new Map();

        if (self.heartbeat) {
            self.heartbeat.stop();
            self.heartbeat = null;
        }

        if (self.socket) {
            self.socket.close();
            self.socket._meteorSession = null;
        }

        setImmediate(function () {
            // Stop callbacks can yield, so we defer this on close.
            // sub._isDeactivated() detects that we set inQueue to null and
            // treats it as semi-deactivated (it will ignore incoming callbacks, etc).
            self._deactivateAllSubscriptions();

            // Defer calling the close callbacks, so that the caller closing
            // the session isn't waiting for all the callbacks to complete.
            for (const callback of self._closeCallbacks) {
                callback();
            }
        });

        // Unregister the session.
        self.server._removeSession(self);
    }

    // Send a message (doing nothing if no socket is connected right now).
    // It should be a JSON object (it will be stringified).
    send(msg) {
        var self = this;
        if (self.socket) {
            self.socket.send(stringifyDDP(msg));
        }
    }

    // Send a connection error.
    sendError(reason, offendingMessage) {
        var self = this;
        var msg = { msg: 'error', reason, offendingMessage };
        self.send(msg);
    }

    // Process 'msg' as an incoming message. As a guard against
    // race conditions during reconnection, ignore the message if
    // 'socket' is not the currently connected socket.
    //
    // We run the messages from the client one at a time, in the order
    // given by the client. The message handler is passed an idempotent
    // function 'unblock' which it may call to allow other messages to
    // begin running in parallel in another fiber (for example, a method
    // that wants to yield). Otherwise, it is automatically unblocked
    // when it returns.
    //
    // Actually, we don't have to 'totally order' the messages in this
    // way, but it's the easiest thing that's correct. (unsub needs to
    // be ordered against sub, methods need to be ordered against each
    // other).
    async processMessage(msg_in: DDPMessage) {
        var self = this;
        if (!self.inQueue) // we have been destroyed.
            return;

        // Respond to ping and pong messages immediately without queuing.
        // If the negotiated DDP version is "pre1" which didn't support
        // pings, preserve the "pre1" behavior of responding with a "bad
        // request" for the unknown messages.
        //
        // Any message counts as receiving a pong, as it demonstrates that
        // the client is still alive.
        if (self.heartbeat) {
            self.heartbeat.messageReceived();
        }

        if (self.version !== 'pre1' && msg_in.msg === 'ping') {
            if (self._respondToPings)
                self.send({ msg: "pong", id: msg_in.id });
            return;
        }
        if (self.version !== 'pre1' && msg_in.msg === 'pong') {
            // Since everything is a pong, there is nothing to do
            return;
        }

        self.inQueue.push(msg_in);
        if (self.workerRunning)
            return;
        self.workerRunning = true;

        while (self.inQueue && self.inQueue.length > 0) {
            var msg = self.inQueue.shift();

            self.server.onMessageHook.each(callback => {
                callback(msg, self);
                return true;
            });

            if (self.protocol_handlers.hasOwnProperty(msg.msg))
                await self.protocol_handlers[msg.msg].call(self, msg);
            else
                self.sendError('Bad request', msg);

        }
        self.workerRunning = false;

    }

    private protocol_handlers = {
        sub: async function (msg: DDPMessage) {
            var self = this;

            // reject malformed messages
            if (typeof (msg.id) !== "string" ||
                typeof (msg.name) !== "string" ||
                (('params' in msg) && !(msg.params instanceof Array))) {
                self.sendError("Malformed subscription", msg);
                return;
            }

            if (!self.server.publish_handlers[msg.name]) {
                self.send({
                    msg: 'nosub', id: msg.id,
                    error: ddpError(404, `Subscription '${msg.name}' not found`)
                });
                return;
            }

            if (self._namedSubs.has(msg.id))
                // subs are idempotent, or rather, they are ignored if a sub
                // with that id already exists. this is important during
                // reconnect.
                return;

            // XXX It'd be much better if we had generic hooks where any package can
            // hook into subscription handling, but in the mean while we special case
            // ddp-rate-limiter package. This is also done for weak requirements to
            // add the ddp-rate-limiter package in case we don't have Accounts. A
            // user trying to use the ddp-rate-limiter must explicitly require it.
            /* if (Package['ddp-rate-limiter']) {
              var DDPRateLimiter = Package['ddp-rate-limiter'].DDPRateLimiter;
              var rateLimiterInput = {
                userId: self.userId,
                clientAddress: self.connectionHandle.clientAddress,
                type: "subscription",
                name: msg.name,
                connectionId: self.id
              };
      
              DDPRateLimiter._increment(rateLimiterInput);
              var rateLimitResult = DDPRateLimiter._check(rateLimiterInput);
              if (!rateLimitResult.allowed) {
                self.send({
                  msg: 'nosub', id: msg.id,
                  error: ddpError(
                    'too-many-requests',
                    DDPRateLimiter.getErrorMessage(rateLimitResult),
                    {timeToReset: rateLimitResult.timeToReset})
                });
                return;
              }
            }*/

            var handler = self.server.publish_handlers[msg.name];

            self._startSubscription(handler, msg.id, msg.params, msg.name);
        },

        unsub: async function (msg: DDPMessage) {
            var self = this;

            self._stopSubscription(msg.id);
        },

        method: async function (msg: DDPMessage) {
            var self = this;

            // Reject malformed messages.
            // For now, we silently ignore unknown attributes,
            // for forwards compatibility.
            if (typeof (msg.id) !== "string" ||
                typeof (msg.method) !== "string" ||
                (('params' in msg) && !(msg.params instanceof Array)) ||
                (('randomSeed' in msg) && (typeof msg.randomSeed !== "string"))) {
                self.sendError("Malformed method invocation", msg);
                return;
            }

            var randomSeed = msg.randomSeed || null;

            // Set up to mark the method as satisfied once all observers
            // (and subscriptions) have reacted to any writes that were
            // done.
            var fence = new _WriteFence();
            fence.onAllCommitted(function () {
                // Retire the fence so that future writes are allowed.
                // This means that callbacks like timers are free to use
                // the fence, and if they fire before it's armed (for
                // example, because the method waits for them) their
                // writes will be included in the fence.
                fence.retire();
                self.send({ msg: 'updated', methods: [msg.id] });
            });

            // Find the handler
            var handler = self.server.method_handlers[msg.method];
            if (!handler) {
                self.send({ msg: 'result', id: msg.id, error: ddpError(404, `Method '${msg.method}' not found`) });
                fence.arm();
                return;
            }

            var setUserId = function (userId) {
                self._setUserId(userId);
            };

            var invocation = new MethodInvocation({
                isSimulation: false,
                userId: self.userId,
                setUserId: setUserId,
                connection: self.connectionHandle,
                randomSeed: randomSeed
            });

            const oldInvocation = DDP._CurrentMethodInvocation;
            const oldFence = _WriteFence._CurrentWriteFence;

            DDP._CurrentMethodInvocation = invocation;
            _WriteFence._CurrentWriteFence = fence;

            function finish() {
                DDP._CurrentMethodInvocation = oldInvocation;
                _WriteFence._CurrentWriteFence = oldFence;
                fence.arm();
            }

            const payload = {
                msg: "result",
                id: msg.id,
                result: undefined,
                error: undefined
            };

            handler.apply(invocation, msg.params).then((result) => {
                finish();
                if (result !== undefined) {
                    payload.result = result;
                }
                self.send(payload);
            }, (exception) => {
                finish();
                if (exception && !exception.isClientSafe)
                    console.error(`Exception while invoking method '${msg.method}'`, exception);
                payload.error = wrapInternalException(
                    exception,
                    `while invoking method '${msg.method}'`
                );
                self.send(payload);
            });
        }
    }

    _eachSub(f) {
        var self = this;
        self._namedSubs.forEach(f);
        self._universalSubs.forEach(f);
    }

    _diffCollectionViews(beforeCVs) {
        var self = this;
        DiffSequence.diffMaps(beforeCVs, self.collectionViews, {
            both: function (collectionName, leftValue, rightValue) {
                rightValue.diff(leftValue);
            },
            rightOnly: function (collectionName, rightValue) {
                rightValue.documents.forEach(function (docView, id) {
                    self.sendAdded(collectionName, id, docView.getFields());
                });
            },
            leftOnly: function (collectionName, leftValue) {
                leftValue.documents.forEach(function (doc, id) {
                    self.sendRemoved(collectionName, id);
                });
            }
        });
    }

    // Sets the current user id in all appropriate contexts and reruns
    // all subscriptions
    _setUserId(userId) {
        var self = this;

        if (userId !== null && typeof userId !== "string")
            throw new Error("setUserId must be called on string or null, not " +
                typeof userId);

        // Prevent newly-created universal subscriptions from being added to our
        // session. They will be found below when we call startUniversalSubs.
        //
        // (We don't have to worry about named subscriptions, because we only add
        // them when we process a 'sub' message. We are currently processing a
        // 'method' message, and the method did not unblock, because it is illegal
        // to call setUserId after unblock. Thus we cannot be concurrently adding a
        // new named subscription).
        self._dontStartNewUniversalSubs = true;

        // Prevent current subs from updating our collectionViews and call their
        // stop callbacks. This may yield.
        self._eachSub(function (sub) {
            sub._deactivate();
        });

        // All subs should now be deactivated. Stop sending messages to the client,
        // save the state of the published collections, reset to an empty view, and
        // update the userId.
        self._isSending = false;
        var beforeCVs = self.collectionViews;
        self.collectionViews = new Map();
        self.userId = userId;

        // _setUserId is normally called from a Meteor method with
        // DDP._CurrentMethodInvocation set. But DDP._CurrentMethodInvocation is not
        // expected to be set inside a publish function, so we temporary unset it.
        // Inside a publish function DDP._CurrentPublicationInvocation is set.
        const oldInvocation = DDP._CurrentMethodInvocation;
        DDP._CurrentMethodInvocation = undefined;
        try {
            // Save the old named subs, and reset to having no subscriptions.
            var oldNamedSubs = self._namedSubs;
            self._namedSubs = new Map();
            self._universalSubs = [];

            oldNamedSubs.forEach(function (sub, subscriptionId) {
                var newSub = sub._recreate();
                self._namedSubs.set(subscriptionId, newSub);
                // nb: if the handler throws or calls this.error(), it will in fact
                // immediately send its 'nosub'. This is OK, though.
                newSub._runHandler();
            });

            // Allow newly-created universal subs to be started on our connection in
            // parallel with the ones we're spinning up here, and spin up universal
            // subs.
            self._dontStartNewUniversalSubs = false;
            self.startUniversalSubs();
        } finally {
            DDP._CurrentMethodInvocation = oldInvocation;
        }

        // Start sending messages again, beginning with the diff from the previous
        // state of the world to the current state. No yields are allowed during
        // this diff, so that other changes cannot interleave.
        self._isSending = true;
        self._diffCollectionViews(beforeCVs);
        if (self._pendingReady.length > 0) {
            self.sendReady(self._pendingReady);
            self._pendingReady = [];
        }
    }

    async _startSubscription(handler, subId?, params?, name?) {
        var self = this;

        var sub = new Subscription(self, handler, subId, params, name);

        if (subId)
            self._namedSubs.set(subId, sub);
        else
            self._universalSubs.push(sub);

        await sub._runHandler();
    }

    // Tear down specified subscription
    _stopSubscription(subId, error?) {
        var subName = null;
        if (subId) {
            var maybeSub = this._namedSubs.get(subId);
            if (maybeSub) {
                subName = maybeSub._name;

                // version 1a doesn't send document deletions and relies on the clients for cleanup
                if (this.version !== "1a")
                    maybeSub._removeAllDocuments();

                maybeSub._deactivate();
                this._namedSubs.delete(subId);
            }
        }

        var response = { msg: 'nosub', id: subId, error: undefined };

        if (error) {
            response.error = wrapInternalException(
                error,
                subName ? ("from sub " + subName + " id " + subId)
                    : ("from sub id " + subId));
        }

        this.send(response);
    }

    // Tear down all subscriptions. Note that this does NOT send removed or nosub
    // messages, since we assume the client is gone.
    _deactivateAllSubscriptions() {
        var self = this;

        self._namedSubs.forEach(function (sub, id) {
            sub._deactivate();
        });
        self._namedSubs = new Map();

        self._universalSubs.forEach(function (sub) {
            sub._deactivate();
        });
        self._universalSubs = [];
    }

    // Determine the remote client's IP address, based on the
    // HTTP_FORWARDED_COUNT environment variable representing how many
    // proxies the server is behind.
    _clientAddress() {
        var self = this;

        // For the reported client address for a connection to be correct,
        // the developer must set the HTTP_FORWARDED_COUNT environment
        // variable to an integer representing the number of hops they
        // expect in the `x-forwarded-for` header. E.g., set to "1" if the
        // server is behind one proxy.
        //
        // This could be computed once at startup instead of every time.
        var httpForwardedCount = parseInt(process.env['HTTP_FORWARDED_COUNT']) || 0;

        if (httpForwardedCount === 0)
            return self.socket.remoteAddress;

        let forwardedFor: string | string[] = self.socket.headers["x-forwarded-for"];
        if (typeof forwardedFor !== "string")
            return null;
        forwardedFor = forwardedFor.trim().split(/\s*,\s*/);

        // Typically the first value in the `x-forwarded-for` header is
        // the original IP address of the client connecting to the first
        // proxy.  However, the end user can easily spoof the header, in
        // which case the first value(s) will be the fake IP address from
        // the user pretending to be a proxy reporting the original IP
        // address value.  By counting HTTP_FORWARDED_COUNT back from the
        // end of the list, we ensure that we get the IP address being
        // reported by *our* first proxy.

        if (httpForwardedCount < 0 || httpForwardedCount > forwardedFor.length)
            return null;

        return forwardedFor[forwardedFor.length - httpForwardedCount];
    }
}
