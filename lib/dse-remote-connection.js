/**
 * Copyright DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';

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
 * Represents a GLV connection.
 */
class DseRemoteConnection extends glv.driver.RemoteConnection{
  /**
   * Creates a new instance of DseRemoteConnection.
   * @param {Client} client
   * @param {GraphQueryOptions} options
   */
  constructor(client, options) {
    super(null);
    this._client = client;
    this._options = options;
  }

  submit(bytecode) {
    return this._executeTraversal(bytecode, this._options);
  }

  _executeTraversal(traversal, options) {
    const query = DseRemoteConnection.getQuery(traversal);
    if (!options) {
      options = defaultGraphOptions;
    }
    else if (options.graphLanguage !== graphLanguageBytecode) {
      options = cloneOptions(options);
      options.graphLanguage = graphLanguageBytecode;
    }
    return this._client.executeGraph(query, null, options).then(toTraversalResult);
  }

  /**
   * Returns the string representation in GraphSON2 format of the traversal
   * @static
   * @param {Traversal} traversal
   * @returns {String}
   */
  static getQuery(traversal) {
    return graphSONWriter.write(traversal);
  }
}

/**
 * @param {GraphResultSet} result
 * @returns {{traversers: Array}}
 */
function toTraversalResult(result) {
  if (result.length === 0) {
    return { traversers: emptyArray };
  }
  const traversers = new Array(result.length);
  result.forEach(function (item, i) {
    traversers[i] = new glv.process.Traverser(graphSONReader.read(item));
  });
  return { traversers };
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
