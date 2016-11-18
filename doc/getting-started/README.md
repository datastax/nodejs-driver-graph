# Getting started

## Configuring a Traversal Execution Profile

The DSE Graph extension takes advantage of [execution profiles][ep] to allow different configurations for the various
query handlers.

You can set the default execution profile to set the [graph name and any graph option][ep-api] and use it like in the
following example:

```javascript
const dse = require('dse-driver');
const dseGraph = require('dse-graph');
const client = new dse.Client({
  contactPoints: ['h1', 'h2'],
  profiles: [
    new dse.ExecutionProfile('default', { graphOptions:  { name: 'my_graph' } })
  ]
});
const g = dseGraph.traversalSource(client);
// Print the names of john's friends
g.V().has('name','john').out('friends').values('name').toList((err, names) => {
  names.forEach(console.log);
});
```

If you have multiple execution profiles, you can also specify it when obtaining the `TraversalSource` instance:

```javascript
const g = dseGraph.traversalSource(client, { executionProfile: 'graph-oltp2' });
```

Visit the [Execution Profiles documentation][ep] and the [Execution Profile API docs][ep-api] on the DSE driver for
more information.


## Graph Traversal Executions via a DSE Driver Client

Queries generated from `Traversal` can also be explicitly executed using the existing 
`client.executeGraph()` method on the [DSE Driver][dse-driver]. `executeGraph()` method returns results
using DSE Graph types. If you are familiar to DSE driver query execution, you might prefer that way.

To generate the query from the traversal, use the `queryFromTraversal()` method:

```javascript
const query = dseGraph.queryFromTraversal(g.V().hasLabel('person'));
```

For the DSE driver to properly execute a query generated from a `Traversal`, you must use an Execution Profile generated
using `createExecutionProfile()` method.

```javascript
const client = new dse.Client({
  contactPoints: ['h1', 'h2'],
  profiles: [
    dseGraph.createExecutionProfile('explicit-exec-graph1')
  ]
});
```

```javascript
const g = dseGraph.traversalSource(client);
const query = dseGraph.queryFromTraversal(g.V().hasLabel('person'));
// Reference the execution profile previously created.
client.executeGraph(query, null, { executionProfile: 'explicit-exec-graph1' }, function(err, result) {
  assert.ifError(err);  
  result.forEach(function (vertex) {
    console.log(vertex.label); // person
    console.log(vertex instanceof dse.Graph.Vertex); // true
  });
});
```

## Putting it all together

```javascript
const dse = require('dse-driver');
const dseGraph = require('dse-graph');
const client = new dse.Client({
  contactPoints: ['h1', 'h2'],
  profiles: [
    new dse.ExecutionProfile('default', { graphOptions:  { name: 'my_graph' } }),
    dseGraph.createExecutionProfile('explicit-exec', { graphOptions:  { name: 'my_graph' } } )
  ]
});
// Obtain a traversal source
const g = dseGraph.traversalSource(client);
// Execute queries using the Traversal toList() method
g.V().has('name','john').out('friends').values('name').toList(function (err, names) {
  names.forEach(console.log);
});

// Alternatively you can convert a given traversal to a string query and use 
// the DSE Driver executeGraph() method
const query = dseGraph.queryFromTraversal(g.V().hasLabel('person'));
// Reference the execution profile previously created.
client.executeGraph(query, null, { executionProfile: 'explicit-exec' }, function(err, result) {
  assert.ifError(err);  
  result.forEach(console.log);
});
```

[dse-driver]: https://github.com/datastax/nodejs-driver-dse
[ep]: http://docs.datastax.com/en/developer/nodejs-driver-dse/latest/features/execution-profiles/
[ep-api]: http://docs.datastax.com/en/drivers/nodejs-dse/latest/ExecutionProfile.html