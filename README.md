MongoDB Live Data Server
========================

This project is essentially a MongoDB live data driver (based either on polling or on Oplog tailing) combined with a DDP server, extracted
out of [Meteor](https://github.com/meteor/meteor), with **Fibers** and **underscore** dependencies removed and code converted to Typescript.

Live data is one of the root concepts of Meteor. Data is served via WebSockets via the DDP protocol and updated automatically whenever something changes in the database. Also, calling server methods via WebSocket is supported.

Using Meteor locks you into the Meteor ecosystem, which has some problems (mostly for historical reasons). Using live data as a separate npm package might be preferable in many scenarios. Also, people who are trying to migrate from Meteor, might find this package useful as an intermediate step.

### Installation

```
npm i mongodb-livedata-server
```

### Usage

As a most common example, this is how you can use livedata with Express.js:

```js
const { DDPServer, LiveCursor, LiveMongoConnection } = require('mongodb-livedata-server')
const express = require('express')
const app = express()
const port = 3000

app.get('/', (req, res) => {
  res.send('Hello World!')
})

const httpServer = app.listen(port, () => {
  console.log(`Example app listening on port ${port}`)
})

const liveMongoConnection = new LiveMongoConnection(process.env.MONGO_URL, {
    oplogUrl: process.env.MONGO_OPLOG_URL
});
const liveDataServer = new DDPServer({}, httpServer);

liveDataServer.methods({
    "test-method": async (msg) => {
        console.log("Test msg: ", msg);
        return "hello! Current timestamp is: " + Date.now()
    }
})

liveDataServer.publish({
    "test-subscription": async () => {
        return new LiveCursor(liveMongoConnection, "test-collection", { category: "apples" });
    }
})

```

`liveDataServer.methods` and `liveDataServer.publish` have exactly same interface as [Meteor.methods](https://docs.meteor.com/api/methods.html#Meteor-methods) and [Meteor.publish](https://docs.meteor.com/api/pubsub.html#Meteor-publish) respectively, notice however that when publishing subscriptions, you must use `LiveCursor` rather than a normal MongoDB cursor.

### Important notes

- The project is in alpha. Use at your own risk.
- Neither method context nor subscription context have the `unblock` method anymore (because this package doesn't use Fibers)
- Meteor syntax for MongoDB queries is not supported. Please always use MongoDB Node.js driver syntax. For example, instead of
  ```ts
  const doc = myCollection.findOne(id);
  ```
  use
  ```ts
  const doc = await myCollection.findOne({ _id: id });
  ```
- Neither MongoDB.ObjectId nor it's Meteor.js alternative is supported at the moment. String ids only.

### DDP Extension

- Starting from 0.1.0, this library extends DDP with `init` message, which is used to avoid initial spam of `added` messages.
- Starting from 0.1.1, `init` message will only be sent if `version` `1a` of DDP protocol is specified. Additionally, when version `1a`
is specified, server will not send removes for all documents when stopping a subscription, and rely on the client for the cleanup instead.
