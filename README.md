# DataStax Enterprise Node.js Driver Extensions for DSE Graph

This package builds on the [DataStax Enterprise Node.js driver][dse-driver], adding functionality for interacting with
DSE graph features and Apache TinkerPop.

DSE Graph Extensions for DataStax Enterprise Node.js Driver can be used solely with [DataStax Enterprise][dse]. Please
consult [the license](#license).

## Installation

```bash
npm install dse-graph
```

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

Copyright 2016 DataStax

http://www.datastax.com/terms/datastax-dse-driver-license-terms

---

_Apache TinkerPop, TinkerPop, Apache are registered trademarks of The Apache Software Foundation_

[dse]: http://www.datastax.com/products/datastax-enterprise
[dse-driver]: https://github.com/datastax/nodejs-driver-dse
[cassandra-driver]: https://github.com/datastax/nodejs-driver
[core-manual]: http://docs.datastax.com/en/latest-nodejs-driver/common/drivers/introduction/introArchOverview.html
[iterable]: https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Iteration_protocols#iterable
[modern-graph]: http://tinkerpop.apache.org/docs/3.1.1-incubating/reference/#_the_graph_structure
[jira]: https://datastax-oss.atlassian.net/projects/NODEJS/issues
[mailing-list]: https://groups.google.com/a/lists.datastax.com/forum/#!forum/nodejs-driver-user
[doc-index]: http://docs.datastax.com/en/latest-dse-nodejs-driver/
[api-docs]: http://docs.datastax.com/en/latest-dse-nodejs-driver-api
[faq]: http://docs.datastax.com/en/developer/nodejs-driver-dse/1.0/supplemental/faq/