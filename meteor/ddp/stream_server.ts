import { Server } from "http";
import deflate from "permessage-deflate";
import sockjs from "sockjs";
import url from "url";
import { DDPSession } from "./session";

export interface StreamServerSocket extends sockjs.Connection {
    setWebsocketTimeout: Function;
    _session?: any;
    _meteorSession?: DDPSession;
    send: Function;
}
// By default, we use the permessage-deflate extension with default
// configuration.

export class StreamServer {
    private registration_callbacks = [];
    private open_sockets: StreamServerSocket[] = [];

    // Because we are installing directly onto WebApp.httpServer instead of using
    // WebApp.app, we have to process the path prefix ourselves.

    private prefix = '/sockjs';
    private server: sockjs.Server;

    constructor(private httpServer: Server) {
        //RoutePolicy.declare(this.prefix + '/', 'network');
        // set up sockjs
        const serverOptions = {
            prefix: this.prefix,
            log: function () { },
            // this is the default, but we code it explicitly because we depend
            // on it in stream_client:HEARTBEAT_TIMEOUT
            heartbeat_delay: 45000,
            // The default disconnect_delay is 5 seconds, but if the server ends up CPU
            // bound for that much time, SockJS might not notice that the user has
            // reconnected because the timer (of disconnect_delay ms) can fire before
            // SockJS processes the new connection. Eventually we'll fix this by not
            // combining CPU-heavy processing with SockJS termination (eg a proxy which
            // converts to Unix sockets) but for now, raise the delay.
            disconnect_delay: 60 * 1000,
            // Set the USE_JSESSIONID environment variable to enable setting the
            // JSESSIONID cookie. This is useful for setting up proxies with
            // session affinity.
            jsessionid: !!process.env.USE_JSESSIONID,

            websocket: true,
            faye_server_options: null
        };

        // If you know your server environment (eg, proxies) will prevent websockets
        // from ever working, set $DISABLE_WEBSOCKETS and SockJS clients (ie,
        // browsers) will not waste time attempting to use them.
        // (Your server will still have a /websocket endpoint.)
        if (process.env.DISABLE_WEBSOCKETS) {
            serverOptions.websocket = false;
        } else {
            serverOptions.faye_server_options = {
                extensions: [deflate.configure({})]
            };
        }

        this.server = sockjs.createServer(serverOptions);

        // Install the sockjs handlers, but we want to keep around our own particular
        // request handler that adjusts idle timeouts while we have an outstanding
        // request.  This compensates for the fact that sockjs removes all listeners
        // for "request" to add its own.
        httpServer.removeListener(
            'request', _timeoutAdjustmentRequestCallback);
        this.server.installHandlers(httpServer);
        httpServer.addListener(
            'request', _timeoutAdjustmentRequestCallback);

        // Support the /websocket endpoint
        this._redirectWebsocketEndpoint();

        this.server.on('connection', (socket: StreamServerSocket) => {
            // sockjs sometimes passes us null instead of a socket object
            // so we need to guard against that. see:
            // https://github.com/sockjs/sockjs-node/issues/121
            // https://github.com/meteor/meteor/issues/10468
            if (!socket) return;

            // We want to make sure that if a client connects to us and does the initial
            // Websocket handshake but never gets to the DDP handshake, that we
            // eventually kill the socket.  Once the DDP handshake happens, DDP
            // heartbeating will work. And before the Websocket handshake, the timeouts
            // we set at the server level in webapp_server.js will work. But
            // faye-websocket calls setTimeout(0) on any socket it takes over, so there
            // is an "in between" state where this doesn't happen.  We work around this
            // by explicitly setting the socket timeout to a relatively large time here,
            // and setting it back to zero when we set up the heartbeat in
            // livedata_server.js.
            socket.setWebsocketTimeout = function (timeout) {
                if ((socket.protocol === 'websocket' ||
                    socket.protocol === 'websocket-raw')
                    && socket._session.recv) {
                    socket._session.recv.connection.setTimeout(timeout);
                }
            };
            socket.setWebsocketTimeout(45 * 1000);

            socket.send = (data) => {
                socket.write(data);
            };
            socket.on('close', () => {
                this.open_sockets = this.open_sockets.filter(s => s !== socket);
            });
            this.open_sockets.push(socket);

            // only to send a message after connection on tests, useful for
            // socket-stream-client/server-tests.js
            if (process.env.TEST_METADATA && process.env.TEST_METADATA !== "{}") {
                socket.send(JSON.stringify({ testMessageOnConnect: true }));
            }

            // call all our callbacks when we get a new socket. they will do the
            // work of setting up handlers and such for specific messages.
            for (const callback of this.registration_callbacks) {
                callback(socket);
            }
        });

    };

    // call my callback when a new socket connects.
    // also call it for all current connections.
    register(callback: (socket: StreamServerSocket) => void) {
        var self = this;
        self.registration_callbacks.push(callback);
        for (const socket of self.all_sockets()) {
            callback(socket);
        }
    }

    // get a list of all sockets
    all_sockets() {
        return Object.values(this.open_sockets);
    }

    // Redirect /websocket to /sockjs/websocket in order to not expose
    // sockjs to clients that want to use raw websockets
    _redirectWebsocketEndpoint() {
        var self = this;
        // Unfortunately we can't use a connect middleware here since
        // sockjs installs itself prior to all existing listeners
        // (meaning prior to any connect middlewares) so we need to take
        // an approach similar to overshadowListeners in
        // https://github.com/sockjs/sockjs-node/blob/cf820c55af6a9953e16558555a31decea554f70e/src/utils.coffee
        ['request', 'upgrade'].forEach((event) => {
            var oldHttpServerListeners = this.httpServer.listeners(event).slice(0);
            this.httpServer.removeAllListeners(event);

            // request and upgrade have different arguments passed but
            // we only care about the first one which is always request
            var newListener = function (request /*, moreArguments */) {
                // Store arguments for use within the closure below
                var args = arguments;

                // Rewrite /websocket and /websocket/ urls to /sockjs/websocket while
                // preserving query string.
                var parsedUrl = url.parse(request.url);
                if (parsedUrl.pathname === '/websocket' ||
                    parsedUrl.pathname === '/websocket/') {
                    parsedUrl.pathname = self.prefix + '/websocket';
                    request.url = url.format(parsedUrl);
                }
                for (const oldListener of oldHttpServerListeners) {
                    oldListener.apply(this.httpServer, args);
                }
            };
            this.httpServer.addListener(event, newListener);
        });
    }
}


const SHORT_SOCKET_TIMEOUT = 5 * 1000;
const LONG_SOCKET_TIMEOUT = 120 * 1000;

// When we have a request pending, we want the socket timeout to be long, to
// give ourselves a while to serve it, and to allow sockjs long polls to
// complete.  On the other hand, we want to close idle sockets relatively
// quickly, so that we can shut down relatively promptly but cleanly, without
// cutting off anyone's response.
function _timeoutAdjustmentRequestCallback(req, res) {
    // this is really just req.socket.setTimeout(LONG_SOCKET_TIMEOUT);
    req.setTimeout(LONG_SOCKET_TIMEOUT);
    // Insert our new finish listener to run BEFORE the existing one which removes
    // the response from the socket.
    var finishListeners = res.listeners('finish');
    // XXX Apparently in Node 0.12 this event was called 'prefinish'.
    // https://github.com/joyent/node/commit/7c9b6070
    // But it has switched back to 'finish' in Node v4:
    // https://github.com/nodejs/node/pull/1411
    res.removeAllListeners('finish');
    res.on('finish', function () {
        res.setTimeout(SHORT_SOCKET_TIMEOUT);
    });
    for (const l of finishListeners) {
        res.on('finish', l);
    }
};