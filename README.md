Livedata Server
===============

This project is essentially a DDP server, extracted out of [Meteor](https://github.com/meteor/meteor), with **Fibers** and **underscore** dependencies removed and code converted to Typescript.

Live data is one of the root concepts of Meteor. Data is served via WebSockets via the DDP protocol and updated automatically whenever something changes in the database. Also, calling server methods via WebSocket is supported.

Using Meteor locks you into the Meteor ecosystem, which has some problems (mostly for historical reasons). Using live data as a separate npm package might be preferable in many scenarios. Also, people who are trying to migrate from Meteor, might find this package useful as an intermediate step.

### Usage

As most common example, this is how you can use livedata with Express.js:

```ts
const express = require('express')
const app = express()
const port = 3000

app.get('/', (req, res) => {
  res.send('Hello World!')
})

const httpServer = app.listen(port, () => {
  console.log(`Example app listening on port ${port}`)
})

const liveDataServer = new DDPServer({}, httpServer);

```

or, with vanilla Node.js HTTP server:

```ts
import { createServer } from "http";
import { DDPServer } from "livedata-server";

const httpServer = createServer().listen(3000);
const liveDataServer = new DDPServer({}, httpServer);
```

After that, you can use `liveDataServer.methods` or `liveDataServer.publish` which have exactly same interface as [Meteor.methods](https://docs.meteor.com/api/methods.html#Meteor-methods) and [Meteor.publish](https://docs.meteor.com/api/pubsub.html#Meteor-publish) respectively.

```ts
liveDataServer.methods({
    "test-method": async (msg) => {
        console.log("Test msg: ", msg);
        return "hello! Current timestamp is: " + Date.now()
    }
})
```

Notable difference from Meteor is that neither method context nor subscription context, don't have `unblock` method anymore (because this package doesn't use Fibers).

Any Meteor client application will be able to connect to this server without any additional configuration.
