# DataStax Enterprise Node.js Driver Extensions for DSE Graph

This package builds on the [DataStax Enterprise Node.js driver][dse-driver], adding functionality for interacting with
DSE graph features and Apache TinkerPop.

DSE Graph Extensions for DataStax Enterprise Node.js Driver can be used solely with [DataStax Enterprise][dse]. Please
consult [the license](#license).

## Installation

```bash
npm install dse-graph
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

Create a `dse.Client` instance and use it to obtain traversal sources: 

```javascript
const dse = require('dse-driver');
const dseGraph = require('dse-graph');

const client = new dse.Client({
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

The full license terms are available at https://www.datastax.com/terms/datastax-dse-driver-license-terms

---

*Apache TinkerPop, TinkerPop, Apache are registered trademarks of The Apache Software Foundation.*

[dse]: https://www.datastax.com/products/datastax-enterprise
[dse-driver]: https://docs.datastax.com/en/developer/nodejs-driver-dse/latest/
[jira]: https://datastax-oss.atlassian.net/projects/NODEJS/issues
[mailing-list]: https://groups.google.com/a/lists.datastax.com/forum/#!forum/nodejs-driver-user
[doc-index]: https://docs.datastax.com/en/developer/nodejs-dse-graph/latest/
[api-docs]: https://docs.datastax.com/en/developer/nodejs-dse-graph/latest/api2/
[getting-started]: https://docs.datastax.com/en/developer/nodejs-dse-graph/latest/getting-started/
[gremlin]: https://tinkerpop.apache.org/docs/3.2.8/reference/#graph-traversal-steps
[gremlin-js-doc]: https://tinkerpop.apache.org/docs/3.2.8/reference/#gremlin-javascript