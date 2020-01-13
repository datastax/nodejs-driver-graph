# Getting Started

## Configuring a Traversal Execution Profile

The DSE Graph extension takes advantage of [execution profiles][ep] to allow different configurations for the various
query handlers.

You can specify the default execution profile to set the [graph name and any graph option][ep-api] and use it like in
 the following example:

```javascript
const { Client, ExecutionProfile } = require('cassandra-driver');
const dseGraph = require('cassandra-driver-graph');

const client = new Client({
  contactPoints: ['host1', 'host2'],
  localDataCenter: 'my_graph_dc',
  profiles: [
    new ExecutionProfile('default', { graphOptions:  { name: 'my_graph' } })
  ]
});

const g = dseGraph.traversalSource(client);
// Print the names of john's friends
g.V().has('name','john').out('friends').values('name').toList()
  .then(names => names.forEach(console.log));
```

If you have multiple execution profiles, you can also specify it when obtaining the `TraversalSource` instance:

```javascript
const g = dseGraph.traversalSource(client, { executionProfile: 'graph-oltp2' });
```

Visit the [Execution Profiles documentation][ep] and the [Execution Profile API docs][ep-api] on the DataStax driver for
more information.


## Graph Traversal Executions via a DataStax Driver Client

Queries generated from `Traversal` can also be explicitly executed using the existing 
`client.executeGraph()` method on the [Driver][driver]. `executeGraph()` method returns results
using DSE Graph types. If you are familiar to driver query execution, you might prefer that way.

To generate the query from the traversal, use the `queryFromTraversal()` method:

```javascript
const query = dseGraph.queryFromTraversal(g.V().hasLabel('person'));
```

For the DataStax driver to properly execute a query generated from a `Traversal`, you must use an Execution Profile
generated using `createExecutionProfile()` method.

```javascript
const client = new Client({
  contactPoints: ['host1', 'host2'],
  localDataCenter: 'my_graph_dc',
  profiles: [
    dseGraph.createExecutionProfile('explicit-exec-graph1')
  ]
});
```

```javascript
const g = dseGraph.traversalSource(client);

const query = dseGraph.queryFromTraversal(g.V().hasLabel('person'));
// Reference the execution profile previously created.
client.executeGraph(query, null, { executionProfile: 'explicit-exec-graph1' })
  .then(result => {
    for (const vertex of result) {
      console.log(vertex.label); // person
      console.log(vertex instanceof cassandra.datastax.Graph.Vertex); // true
    }
  });
```

## Putting it all together

```javascript
const { Client, ExecutionProfile } = require('cassandra-driver');
const dseGraph = require('cassandra-driver-graph');
const client = new Client({
  contactPoints: ['host1', 'host2'],
  localDataCenter: 'my_graph_dc',
  profiles: [
    new ExecutionProfile('default', { graphOptions:  { name: 'my_graph' } }),
    dseGraph.createExecutionProfile('explicit-exec', { graphOptions:  { name: 'my_graph' } } )
  ]
});

// Obtain a traversal source
const g = dseGraph.traversalSource(client);

// Execute queries using the Traversal toList() method
g.V().hasLabel('person').values('age').toList()
  .then(ages => ages.forEach(console.log));

// Alternatively you can convert a given traversal to a string query and use 
// the DataStax driver executeGraph() method
const query = dseGraph.queryFromTraversal(g.V().hasLabel('person').values('age'));
// Reference the execution profile previously created.
client.executeGraph(query, null, { executionProfile: 'explicit-exec' })
  .then(result => console.log(result.toArray()));
```

[driver]: https://github.com/datastax/nodejs-driver
[ep]: https://docs.datastax.com/en/developer/nodejs-driver/latest/features/execution-profiles/
[ep-api]: https://docs.datastax.com/en/developer/nodejs-driver/latest/api/class.ExecutionProfile/