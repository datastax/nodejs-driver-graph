/**
 * Copyright DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';

const util = require('util');
const glv = require('gremlin-javascript');
const serializationExtensions = require('./serialization-extensions');

/** @type {Array} */
const emptyArray = Object.freeze([]);
const customSerializers = serializationExtensions.getGlvSerializers();
const graphSONWriter = new glv.structure.io.GraphSONWriter({ serializers: customSerializers });
const graphSONReader = new glv.structure.io.GraphSONReader({ serializers: customSerializers });
const graphLanguageBytecode = 'bytecode-json';
const defaultGraphOptions = Object.freeze({ graphLanguage: graphLanguageBytecode });

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
  const query = DseRemoteConnection.getQuery(traversal);
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
  const traversers = new Array(result.length);
  result.forEach(function (item, i) {
    traversers[i] = graphSONReader.read(item);
  });
  return traversers;
}

function cloneOptions(userOptions) {
  const result = {};
  const userOptionsKeys = Object.keys(userOptions);
  let key, value;
  let i = userOptionsKeys.length;
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
