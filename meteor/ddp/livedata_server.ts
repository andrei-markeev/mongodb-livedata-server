import { MethodInvocation } from "./method-invocation";
import { StreamServer, StreamServerSocket } from "./stream_server";
import { DDPSession } from "./session";
import { Server } from "http";
import { parseDDP, stringifyDDP, SUPPORTED_DDP_VERSIONS } from "./utils";
import { makeRpcSeed } from "./random-stream";
import { clone } from "../ejson/ejson";
import { Hook } from "../callback-hook/hook";
import { Subscription } from "./subscription";

export const DDP: {
    _CurrentPublicationInvocation: Subscription,
    _CurrentMethodInvocation: MethodInvocation
} = {} as any;

// This file contains classes:
// * Session - The server's connection to a single DDP client
// * Subscription - A single subscription for a single client
// * Server - An entire server that may talk to > 1 client. A DDP endpoint.
//
// Session and Subscription are file scope. For now, until we freeze
// the interface, Server is package scope (in the future it should be
// exported).


/******************************************************************************/
/* Server                                                                     */
/******************************************************************************/

interface PublicationStrategy {
    useCollectionView: boolean;
    doAccountingForCollection: boolean;
}

export class DDPServer {
    private options: {
        heartbeatInterval: number;
        heartbeatTimeout: number;
        // For testing, allow responding to pings to be disabled.
        respondToPings: boolean;
        defaultPublicationStrategy: PublicationStrategy;
    };

    private onConnectionHook: Hook;
    private onMessageHook: Hook;

    private publish_handlers: Record<string, any> = {};
    private universal_publish_handlers = [];
    private method_handlers = {};
    private _publicationStrategies = {};
    private sessions = new Map<string, DDPSession>(); // map from id to session
    private stream_server: StreamServer;

    constructor (options = {}, httpServer: Server) {
        var self = this;

        // The default heartbeat interval is 30 seconds on the server and 35
        // seconds on the client.  Since the client doesn't need to send a
        // ping as long as it is receiving pings, this means that pings
        // normally go from the server to the client.
        //
        // Note: Troposphere depends on the ability to mutate
        // Meteor.server.options.heartbeatTimeout! This is a hack, but it's life.
        self.options = {
            heartbeatInterval: 15000,
            heartbeatTimeout: 15000,
            // For testing, allow responding to pings to be disabled.
            respondToPings: true,
            defaultPublicationStrategy: DDPServer.publicationStrategies.SERVER_MERGE,
            ...options,
        };

        // Map of callbacks to call when a new connection comes in to the
        // server and completes DDP version negotiation. Use an object instead
        // of an array so we can safely remove one from the list while
        // iterating over it.
        self.onConnectionHook = new Hook({
            debugPrintExceptions: "onConnection callback"
        });

        // Map of callbacks to call when a new message comes in.
        self.onMessageHook = new Hook({
            debugPrintExceptions: "onMessage callback"
        });


        self.stream_server = new StreamServer(httpServer);

        self.stream_server.register(function (socket) {
            // socket implements the SockJSConnection interface
            socket._meteorSession = null;

            var sendError = function (reason, offendingMessage?) {
                var msg = { msg: 'error', reason, offendingMessage };
                socket.send(stringifyDDP(msg));
            };

            socket.on('data', async function (raw_msg) {
                try {
                    try {
                        var msg = parseDDP(raw_msg);
                    } catch (err) {
                        sendError('Parse error');
                        return;
                    }
                    if (msg === null || !msg.msg) {
                        sendError('Bad request', msg);
                        return;
                    }

                    if (msg.msg === 'connect') {
                        if (socket._meteorSession) {
                            sendError("Already connected", msg);
                            return;
                        }
                        self._handleConnect(socket, msg);
                        return;
                    }

                    if (!socket._meteorSession) {
                        sendError('Must connect first', msg);
                        return;
                    }
                    await socket._meteorSession.processMessage(msg);
                } catch (e) {
                    // XXX print stack nicely
                    console.log("Internal exception while processing message", msg, e);
                }
            });

            socket.on('close', function () {
                if (socket._meteorSession) {
                    socket._meteorSession.close();
                }
            });
        });
    }

    // Publication strategies define how we handle data from published cursors at the collection level
    // This allows someone to:
    // - Choose a trade-off between client-server bandwidth and server memory usage
    // - Implement special (non-mongo) collections like volatile message queues
    public static publicationStrategies = {
        // SERVER_MERGE is the default strategy.
        // When using this strategy, the server maintains a copy of all data a connection is subscribed to.
        // This allows us to only send deltas over multiple publications.
        SERVER_MERGE: {
            useCollectionView: true,
            doAccountingForCollection: true,
        },
        // The NO_MERGE_NO_HISTORY strategy results in the server sending all publication data
        // directly to the client. It does not remember what it has previously sent
        // to it will not trigger removed messages when a subscription is stopped.
        // This should only be chosen for special use cases like send-and-forget queues.
        NO_MERGE_NO_HISTORY: {
            useCollectionView: false,
            doAccountingForCollection: false,
        },
        // NO_MERGE is similar to NO_MERGE_NO_HISTORY but the server will remember the IDs it has
        // sent to the client so it can remove them when a subscription is stopped.
        // This strategy can be used when a collection is only used in a single publication.
        NO_MERGE: {
            useCollectionView: false,
            doAccountingForCollection: true,
        }
    }

    /**
     * @summary Register a callback to be called when a new DDP connection is made to the server.
     * @locus Server
     * @param {function} callback The function to call when a new DDP connection is established.
     * @memberOf Meteor
     * @importFromPackage meteor
     */
    onConnection(fn) {
        var self = this;
        return self.onConnectionHook.register(fn);
    }

    /**
     * @summary Set publication strategy for the given publication. Publications strategies are available from `DDPServer.publicationStrategies`. You call this method from `Meteor.server`, like `Meteor.server.setPublicationStrategy()`
     * @locus Server
     * @alias setPublicationStrategy
     * @param publicationName {String}
     * @param strategy {{useCollectionView: boolean, doAccountingForCollection: boolean}}
     * @memberOf Meteor.server
     * @importFromPackage meteor
     */
    setPublicationStrategy(publicationName: string, strategy: PublicationStrategy) {
        if (!Object.values(DDPServer.publicationStrategies).includes(strategy)) {
            throw new Error(`Invalid merge strategy: ${strategy} 
        for collection ${publicationName}`);
        }
        this._publicationStrategies[publicationName] = strategy;
    }

    /**
     * @summary Gets the publication strategy for the requested publication. You call this method from `Meteor.server`, like `Meteor.server.getPublicationStrategy()`
     * @locus Server
     * @alias getPublicationStrategy
     * @param publicationName {String}
     * @memberOf Meteor.server
     * @importFromPackage meteor
     * @return {{useCollectionView: boolean, doAccountingForCollection: boolean}}
     */
    getPublicationStrategy(publicationName: string): PublicationStrategy {
        return this._publicationStrategies[publicationName]
            || this.options.defaultPublicationStrategy;
    }

    /**
     * @summary Register a callback to be called when a new DDP message is received.
     * @locus Server
     * @param {function} callback The function to call when a new DDP message is received.
     * @memberOf Meteor
     * @importFromPackage meteor
     */
    onMessage(fn) {
        var self = this;
        return self.onMessageHook.register(fn);
    }

    _handleConnect(socket: StreamServerSocket, msg: any) {
        var self = this;

        // The connect message must specify a version and an array of supported
        // versions, and it must claim to support what it is proposing.
        if (!(
                typeof (msg.version) === 'string' &&
                Array.isArray(msg.support) &&
                msg.support.every(s => typeof s === "string") &&
                msg.support.includes(msg.version)
            ))
        {
            socket.send(stringifyDDP({
                msg: 'failed',
                version: SUPPORTED_DDP_VERSIONS[0]
            }));
            socket.close();
            return;
        }

        // In the future, handle session resumption: something like:
        //  socket._meteorSession = self.sessions[msg.session]
        var version = _calculateVersion(msg.support, SUPPORTED_DDP_VERSIONS);

        if (msg.version !== version) {
            // The best version to use (according to the client's stated preferences)
            // is not the one the client is trying to use. Inform them about the best
            // version to use.
            socket.send(stringifyDDP({ msg: 'failed', version: version }));
            socket.close();
            return;
        }

        // Yay, version matches! Create a new session.
        // Note: Troposphere depends on the ability to mutate
        // Meteor.server.options.heartbeatTimeout! This is a hack, but it's life.
        socket._meteorSession = new DDPSession(self, version, socket, self.options);
        self.sessions.set(socket._meteorSession.id, socket._meteorSession);
        self.onConnectionHook.each(function (callback) {
            if (socket._meteorSession)
                callback(socket._meteorSession.connectionHandle);
            return true;
        });
    }

    /**
     * Register a publish handler function.
     *
     * @param name {String} identifier for query
     * @param handler {Function} publish handler
     *
     * Server will call handler function on each new subscription,
     * either when receiving DDP sub message for a named subscription, or on
     * DDP connect for a universal subscription.
     *
     * If name is null, this will be a subscription that is
     * automatically established and permanently on for all connected
     * client, instead of a subscription that can be turned on and off
     * with subscribe().
     *
     */

    /**
     * @summary Publish a record set.
     * @memberOf Meteor
     * @importFromPackage meteor
     * @locus Server
     * @param {String|Object} name If String, name of the record set.  If Object, publications Dictionary of publish functions by name.  If `null`, the set has no name, and the record set is automatically sent to all connected clients.
     * @param {Function} func Function called on the server each time a client subscribes.  Inside the function, `this` is the publish handler object, described below.  If the client passed arguments to `subscribe`, the function is called with the same arguments.
     */
    publish(name: string | Record<string, (this: MethodInvocation, ...args: any[]) => Promise<any>> | null, handler?: (this: MethodInvocation, ...args: any[]) => Promise<any>) {
        var self = this;

        if (typeof name === "string") {
            if (name in self.publish_handlers) {
                console.log("Ignoring duplicate publish named '" + name + "'");
                return;
            }

            self.publish_handlers[name] = handler;
        } else if (name == null) {
            self.universal_publish_handlers.push(handler);
            // Spin up the new publisher on any existing session too. Run each
            // session's subscription in a new Fiber, so that there's no change for
            // self.sessions to change while we're running this loop.
            self.sessions.forEach(function (session) {
                if (!session._dontStartNewUniversalSubs) {
                    session._startSubscription(handler);
                }
            });
        }
        else {
            for (const [key, value] of Object.entries(name)) {
                self.publish(key, value);
            }
        }
    }

    _removeSession(session: DDPSession) {
        this.sessions.delete(session.id);
    }

    /**
     * @summary Defines functions that can be invoked over the network by clients.
     * @locus Anywhere
     * @param {Object} methods Dictionary whose keys are method names and values are functions.
     * @memberOf Meteor
     * @importFromPackage meteor
     */
    methods(methods: Record<string, (this: MethodInvocation, ...args: any[]) => Promise<any>>) {
        var self = this;
        for (const [name, func] of Object.entries(methods)) {
            if (typeof func !== 'function')
                throw new Error("Method '" + name + "' must be a function");
            if (self.method_handlers[name])
                throw new Error("A method named '" + name + "' is already defined");
            self.method_handlers[name] = func;
        }
    }

    // A version of the call method that always returns a Promise.
    callAsync(name: string, ...args: any[]) {
        return this.applyAsync(name, args);
    }

    // @param options {Optional Object}
    applyAsync(name: string, args: any[]) {
        // Run the handler
        var handler = this.method_handlers[name];
        if (!handler) {
            return Promise.reject(
                ddpError(404, `Method '${name}' not found`)
            );
        }

        // If this is a method call from within another method or publish function,
        // get the user state from the outer method or publish function, otherwise
        // don't allow setUserId to be called
        var userId = null;
        var setUserId: (userId: string) => void = function () {
            throw new Error("Can't call setUserId on a server initiated method call");
        };
        var connection = null;
        var currentMethodInvocation = DDP._CurrentMethodInvocation;
        var currentPublicationInvocation = DDP._CurrentPublicationInvocation;
        var randomSeed = null;
        if (currentMethodInvocation) {
            userId = currentMethodInvocation.userId;
            setUserId = function (userId) {
                currentMethodInvocation.setUserId(userId);
            };
            connection = currentMethodInvocation.connection;
            randomSeed = makeRpcSeed(currentMethodInvocation, name);
        } else if (currentPublicationInvocation) {
            userId = currentPublicationInvocation.userId;
            setUserId = function (userId) {
                currentPublicationInvocation._session._setUserId(userId);
            };
            connection = currentPublicationInvocation.connection;
        }

        var invocation = new MethodInvocation({
            isSimulation: false,
            userId,
            setUserId,
            connection,
            randomSeed
        });

        return new Promise(resolve => {
            const oldInvocation = DDP._CurrentMethodInvocation;
            try {
                DDP._CurrentMethodInvocation = invocation;
                const result = maybeAuditArgumentChecks(handler, invocation, clone(args), "internal call to '" + name + "'");
                resolve(result);
            } finally {
                DDP._CurrentMethodInvocation = oldInvocation;
            }
        }).then(clone);
    }

    _urlForSession(sessionId) {
        var self = this;
        var session = self.sessions.get(sessionId);
        if (session)
            return session._socketUrl;
        else
            return null;
    }
}

export function _calculateVersion(clientSupportedVersions, serverSupportedVersions) {
    var correctVersion = clientSupportedVersions.find(version => serverSupportedVersions.includes(version));
    if (!correctVersion) {
        correctVersion = serverSupportedVersions[0];
    }
    return correctVersion;
};


// "blind" exceptions other than those that were deliberately thrown to signal
// errors to the client
export function wrapInternalException(exception, context) {
    if (!exception) return exception;

    // To allow packages to throw errors intended for the client but not have to
    // depend on the Meteor.Error class, `isClientSafe` can be set to true on any
    // error before it is thrown.
    if (exception.isClientSafe) {
        if (!(exception instanceof ClientSafeError)) {
            const originalMessage = exception.message;
            exception = ddpError(exception.error, exception.reason, exception.details);
            exception.message = originalMessage;
        }
        return exception;
    }

    // Did the error contain more details that could have been useful if caught in
    // server code (or if thrown from non-client-originated code), but also
    // provided a "sanitized" version with more context than 500 Internal server
    // error? Use that.
    if (exception.sanitizedError) {
        if (exception.sanitizedError.isClientSafe)
            return exception.sanitizedError;
    }

    return ddpError(500, "Internal server error");
};


// Audit argument checks, if the audit-argument-checks package exists (it is a
// weak dependency of this package).
export function maybeAuditArgumentChecks(f: Function, context: any, args: any[] | null, description: string) {
    args = args || [];
    /*if (Package['audit-argument-checks']) {
        return Match._failIfArgumentsAreNotAllChecked(
            f, context, args, description);
    }*/
    return f.apply(context, args);
};

export function ddpError(error: string | number, reason?: string, details?: string) {
    return { isClientSafe: true, error, reason, message: (reason ? reason + " " : "") + "[" + error + "]", errorType: "Meteor.Error" };
}

export class ClientSafeError extends Error {
    constructor(public error: string | number, public reason?: string, public details?: string) {
        super((reason ? reason + " " : "") + "[" + error + "]");
    }
    public isClientSafe = true;
    public errorType = "Meteor.Error";
}