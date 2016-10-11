/**
 * Copyright (C) 2016 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';

var glv = require('./lib/tinkerpop');
var DseRemoteConnection = require('./lib/dse-remote-connection');

module.exports = {
  /**
   * Creates a new graph traversal source.
   * @param {Client} client The <code>dse.Client</code> instance.
   * @param {GraphQueryOptions} [options] The graph query options.
   * @returns {GraphTraversalSource} Returns an Apache TinkerPop GraphTraversalSource.
   */
  graphTraversalSource: function (client, options) {
    var traversalStrategies = new glv.process.TraversalStrategies();
    var remoteConnection = new DseRemoteConnection(client, options);
    traversalStrategies.addStrategy(new glv.driver.RemoteStrategy(remoteConnection));
    return new glv.process.GraphTraversalSource(null, traversalStrategies);
  },
  version: require('./package.json').version
};