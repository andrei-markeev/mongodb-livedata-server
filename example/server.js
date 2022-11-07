/// @ts-check

const { createServer } = require("http");
const { DDPServer } = require("../dist/meteor/ddp/livedata_server");
const fs = require("fs");
const path = require("path");

const server = createServer(httpListener).listen(3000);

function httpListener(request, response) {
    console.log(request.method, request.url);
    var filePath = './public/' + request.url.replace(/^[\.\/\\]+/, "");
    fs.stat(filePath, (err, stats) => {
        if (!err && stats.isFile())
            return getStaticFile(filePath, response);
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

const liveDataServer = new DDPServer({}, server);

liveDataServer.methods({
    "test-method": async (msg) => { console.log("Test msg: ", msg); return "hello! Current timestamp is: " + Date.now() }
})