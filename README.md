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


## Getting Help

You can use the [project mailing list][mailing-list] or create a ticket on the [Jira issue tracker][jira]. 

## Getting Started

Create a `dse.Client` instance and use it to obtain traversal sources: 

```javascript
const dse = require('dse-driver');
const dseGraph = require('dse-graph');
const client = new dse.Client({ contactPoints: ['h1', 'h2'] });
const g = dseGraph.traversalSource(client);
// print john's friends names
g.V().has('name','john').out('friends').values('name').toList((err, names) => {
  names.forEach(console.log);
});
```

You should reuse the `Client` instance across your application.

## License

Copyright 2016-2017 DataStax

http://www.datastax.com/terms/datastax-dse-driver-license-terms

---

_Apache TinkerPop, TinkerPop, Apache are registered trademarks of The Apache Software Foundation_

[dse]: http://www.datastax.com/products/datastax-enterprise
[dse-driver]: https://github.com/datastax/nodejs-driver-dse
[jira]: https://datastax-oss.atlassian.net/projects/NODEJS/issues
[mailing-list]: https://groups.google.com/a/lists.datastax.com/forum/#!forum/nodejs-driver-user
[doc-index]: http://docs.datastax.com/en/developer/nodejs-dse-graph/latest/
[api-docs]: http://docs.datastax.com/en/developer/nodejs-dse-graph/latest/api2/
[getting-started]: http://docs.datastax.com/en/developer/nodejs-dse-graph/latest/getting-started/