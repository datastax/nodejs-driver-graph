# DataStax Node.js Driver Extensions for DSE Graph

This package builds on the [DataStax Node.js Driver for Apache Cassandra][driver], adding functionality for
interacting with DSE graph features and Apache TinkerPop.

## Installation

```bash
npm install cassandra-driver-graph
```

## Documentation

- [Getting started][getting-started]
- [Documentation index][doc-index]
- [API docs][api-docs]
- [Gremlin Reference][gremlin]
- [Gremlin-JavaScript Reference][gremlin-js-doc]

## Getting Help

You can use the [project mailing list][mailing-list] or create a ticket on the [Jira issue tracker][jira]. 

## Basic Usage

Create a `Client` instance and use it to obtain traversal sources: 

```javascript
const { Client } = require('cassandra-driver');
const dseGraph = require('cassandra-driver-graph');

const client = new Client({
  contactPoints: ['host1', 'host2'],
  graphOptions:  { name: 'my_graph' }
});

// Obtain a traversal source, used to create traversals
const g = dseGraph.traversalSource(client);

// Use the traversal source to create traversals
// ie: Print john's friends names
g.V().has('name','john').out('friends').values('name').toList()
  .then(names => names.forEach(console.log));
```

You should reuse the `Client` instance across your application.

Read the full [Getting Started Guide][getting-started].

## License

© DataStax, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed
on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
for the specific language governing permissions and limitations under the License.

---

*Apache TinkerPop, TinkerPop, Apache are registered trademarks of The Apache Software Foundation.*

[dse]: https://www.datastax.com/products/datastax-enterprise
[driver]: https://github.com/datastax/nodejs-driver
[jira]: https://datastax-oss.atlassian.net/projects/NODEJS/issues
[mailing-list]: https://groups.google.com/a/lists.datastax.com/forum/#!forum/nodejs-driver-user
[doc-index]: https://docs.datastax.com/en/developer/nodejs-dse-graph/latest/
[api-docs]: https://docs.datastax.com/en/developer/nodejs-dse-graph/latest/api2/
[getting-started]: https://docs.datastax.com/en/developer/nodejs-dse-graph/latest/getting-started/
[gremlin]: https://tinkerpop.apache.org/docs/3.2.9/reference/#graph-traversal-steps
[gremlin-js-doc]: https://tinkerpop.apache.org/docs/3.2.9/reference/#gremlin-javascript