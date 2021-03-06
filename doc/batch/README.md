# Batch Support

This package supports batching multiple graph updates into a single transaction. All mutations
included in a batch will be applied if the execution completes successfully or none of them if any of the
operations fail.

For the DataStax driver to execute traversal batches, you must use an [execution profile][ep] generated using 
`createExecutionProfile()` method.

```javascript
const client = new Client({
  contactPoints: ['host1', 'host2'],
  localDataCenter: 'graph_dc',
  profiles: [
    dseGraph.createExecutionProfile('my-batch-profile', { graphOptions:  { name: 'my_graph' } })
  ]
});
```

Get a traversal source

```javascript
const g = dseGraph.traversalSource(client);
```

Create an array containing the batch traversals.

```javascript
const batch = [
  g.addV('person').property('name', 'Matt').property('age', 12),
  g.addV('person').property('name', 'Olivia').property('age', 8)
];
```

Use `queryFromBatch()` to obtain the query to execute.

```javascript
const query = dseGraph.queryFromBatch(batch);
```

Use `executeGraph()` method from the [DataStax Driver][driver] to execute the batch, using the execution profile 
previously created using `createExecutionProfile()`.
 
 ```javascript
client.executeGraph(query, null, { executionProfile: 'my-batch-profile' })
  .then(() => console.log('Batch executed successfully'));
```

## Batch options

It's recommended that you specify the batch options like consistency, timeout and other settings at execution profile 
level, when creating the `Client` instance.

```javascript
const client = new Client({
  contactPoints: ['host1', 'host2'],
  localDataCenter: 'my_graph_dc',
  profiles: [
    dseGraph.createExecutionProfile('batch-quorum', { 
      graphOptions:  { name: 'my_graph' },
      consistency: types.consistencies.localQuorum
    })
  ]
});
```

You can reuse execution profiles across different `executeGraph()` calls when settings are the same. 

```javascript
client.executeGraph(otherBatchQuery, null, { executionProfile: 'batch-quorum' });
```

## Complete code sample

```javascript
const { Client, types } = require('cassandra-driver');
const dseGraph = require('cassandra-driver-graph');
const gremlin = require('gremlin');
const __ = gremlin.process.statics;

// Create an instance of Client and reuse it across your application
const client = new Client({
  contactPoints: ['host1', 'host2'],
  localDataCenter: 'my_graph_dc',
  profiles: [
    dseGraph.createExecutionProfile('batch-quorum', { 
      graphOptions:  { name: 'my_graph' },
      consistency: types.consistencies.localQuorum
    })
  ]
});

// Get a traversal source
const g = dseGraph.traversalSource(client);

// Create an array containing the traversals
const batch = [
  g.addV('person').property('name', 'Matt').property('age', 12),
  g.addV('person').property('name', 'Olivia').property('age', 8),
  g.V().has('name', 'Matt').addE('knows').to(__.V().has('name', 'Olivia'))
];

// Obtain the query to execute
const query = dseGraph.queryFromBatch(batch);

// Execute using the Driver with an execution profile.
client.executeGraph(query, null, { executionProfile: 'batch-quorum' });
```

[driver]: https://github.com/datastax/nodejs-driver
[ep]: https://docs.datastax.com/en/developer/nodejs-driver/latest/features/execution-profiles/
