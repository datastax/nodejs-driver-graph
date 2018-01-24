/**
 * Copyright (C) 2016 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';

var util = require('util');
var glv = require('gremlin-javascript');
var serializationExtensions = require('./serialization-extensions');

/** @type {Array} */
var emptyArray = Object.freeze([]);
var customSerializers = serializationExtensions.getGlvSerializers();
var graphSONWriter = new glv.structure.io.GraphSONWriter({ serializers: customSerializers });
var graphSONReader = new glv.structure.io.GraphSONReader({ serializers: customSerializers });
var graphLanguageBytecode = 'bytecode-json';
var defaultGraphOptions = Object.freeze({ graphLanguage: graphLanguageBytecode });

/**
 * @param {Client} client
 * @param {GraphQueryOptions} options
 * @extends {RemoteConnection}
 * @constructor
 */
function DseRemoteConnection(client, options) {
  glv.driver.RemoteConnection.call(this, null, null);
  this._client = client;
  this._options = options;
}

util.inherits(DseRemoteConnection, glv.driver.RemoteConnection);

DseRemoteConnection.prototype.submit = function (bytecode, callback) {
  this._executeTraversal(bytecode, this._options, callback);
};

DseRemoteConnection.prototype._executeTraversal = function (traversal, options, callback) {
  var query = DseRemoteConnection.getQuery(traversal);
  if (!options) {
    options = defaultGraphOptions;
  }
  else if (options.graphLanguage !== graphLanguageBytecode) {
    options = cloneOptions(options);
    options.graphLanguage = graphLanguageBytecode;
  }
  this._client.executeGraph(query, null, options, function (err, result) {
    if (err) {
      return callback(err);
    }
    callback(null, { traversers: toTraversalResult(result) });
  });
};

/**
 * Returns the string representation in GraphSON2 format of the traversal
 * @static
 * @param {Traversal} traversal
 * @returns {String}
 */
DseRemoteConnection.getQuery = function (traversal) {
  return graphSONWriter.write(traversal);
};

/**
 * @param {GraphResultSet} result
 * @returns {Array}
 */
function toTraversalResult(result) {
  if (result.length === 0) {
    return emptyArray;
  }
  var traversers = new Array(result.length);
  result.forEach(function (item, i) {
    traversers[i] = graphSONReader.read(item);
  });
  return traversers;
}

function cloneOptions(userOptions) {
  var result = {};
  var userOptionsKeys = Object.keys(userOptions);
  var key, value;
  var i = userOptionsKeys.length;
  while (i--) {
    key = userOptionsKeys[i];
    value = userOptions[key];
    if (value === undefined) {
      continue;
    }
    result[key] = value;
  }
  return result;
}

module.exports = DseRemoteConnection;
