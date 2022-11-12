/// @ts-check

const { createServer } = require("http");
const fs = require("fs");
const path = require("path");

const { DDPServer } = require("../dist/meteor/ddp/livedata_server");
const { LiveMongoConnection } = require("../dist/meteor/mongo/live_connection");
const { LiveCursor } = require("../dist/meteor/mongo/live_cursor");
const { randomBytes } = require("crypto");

const server = createServer(httpListener).listen(3000);

const liveDataServer = new DDPServer({}, server);

const mongoUrl = process.env.MONGO_URL;
const oplogUrl = process.env.MONGO_OPLOG_URL;

const liveMongoConnection = new LiveMongoConnection(mongoUrl, { oplogUrl });

const testCollection = liveMongoConnection.db.collection("test-collection");

liveDataServer.methods({
    "test-method": async (msg) => {
        console.log("Test msg: ", msg);
        return "hello! Current timestamp is: " + Date.now()
    },
    "test-add": async (category, value) => {
        /// @ts-ignore-next-line
        await testCollection.insertOne({ _id: randomBytes(8).toString("hex"), category, value });
    },
    "test-update-value": async (_id, value) => {
        await testCollection.updateOne({ _id }, { $set: { value } });
    },
    "test-update-category": async (_id, category) => {
        await testCollection.updateOne({ _id }, { $set: { category } });
    },
    "test-remove": async (_id) => {
        await testCollection.deleteOne({ _id });
    }
})

liveDataServer.publish({
    "test-collection": async () => {
        return new LiveCursor(liveMongoConnection, "test-collection", { category: "apples" });
    }
})

function httpListener(request, response) {
    console.log(request.method, request.url);
    var filePath = './public/' + request.url.replace(/^[\.\/\\]+/, "");
    fs.stat(filePath, (err, stats) => {
        if (!err && stats.isFile())
            return getStaticFile(filePath, response);

        response.writeHead(404);
        response.end();
    });
}

function getStaticFile(filePath, response) {
    var extname = String(path.extname(filePath)).toLowerCase();
    var mimeTypes = {
        '.html': 'text/html',
        '.js': 'text/javascript',
        '.css': 'text/css',
        '.png': 'image/png',
        '.jpg': 'image/jpg',
        '.gif': 'image/gif',
        '.svg': 'image/svg+xml'
    };

    var contentType = mimeTypes[extname] || 'application/octet-stream';

    fs.readFile(filePath, function (error, content) {
        if (error) {
            if (error.code == 'ENOENT') {
                response.writeHead(404, { 'Content-Type': 'text/plain' });
                response.end("File not found");
            }
            else {
                response.writeHead(500);
                response.end('Internal server error');
            }
        } else {
            response.writeHead(200, { 'Content-Type': contentType });
            response.end(content, 'utf-8');
        }
    });

}
