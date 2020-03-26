/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

const glv = require('gremlin');
const serializationExtensions = require('./serialization-extensions');
const { GraphSON2Writer, GraphSON2Reader, GraphSON3Writer, GraphSON3Reader} = glv.structure.io;

const graphLanguageBytecode = 'bytecode-json';

const graphProtocol = Object.freeze({
  graphson1: 'graphson-1.0',
  graphson2: 'graphson-2.0',
  graphson3: 'graphson-3.0'
});

const graphSON2Writer = new GraphSON2Writer({ serializers: serializationExtensions.getGlvSerializers() });
const graphSON2Reader = new GraphSON2Reader({ serializers: serializationExtensions.getGlvSerializers() });
const graphSON3Writer = new GraphSON3Writer({ serializers: serializationExtensions.getGlvSerializers() });
const graphSON3Reader = new GraphSON3Reader({ serializers: serializationExtensions.getGlvSerializers() });

const queryWriters = new Map([
  [ graphProtocol.graphson2, getQueryWriter(graphSON2Writer) ],
  [ graphProtocol.graphson3, getQueryWriter(graphSON3Writer) ]
]);

const rowParsers = new Map([
  [ graphProtocol.graphson2, getRowParser(graphSON2Reader) ],
  [ graphProtocol.graphson3, getRowParser(graphSON3Reader) ]
]);

const defaultQueryWriter = queryWriters.get(graphProtocol.graphson2);
const defaultGraphOptions = {
  graphLanguage: graphLanguageBytecode,
  queryWriterFactory,
  rowParserFactory
};

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
    this._createOptions(options);
  }

  submit(bytecode) {
    return this._executeTraversal(bytecode, this._options);
  }

  async _executeTraversal(traversal) {
    const result = await this._client.executeGraph(traversal, null, this._options);
    return new glv.driver.RemoteTraversal(Array.from(result.getTraversers()));
  }

  _createOptions(userOptions) {
    if (!userOptions) {
      this._options = defaultGraphOptions;
      return;
    }

    this._options = Object.assign({
      queryWriterFactory,
      rowParserFactory
    }, userOptions);

    if (userOptions.graphLanguage !== graphLanguageBytecode) {
      this._options.graphLanguage = graphLanguageBytecode;
    }
  }

  /**
   * Returns the string representation in GraphSON2/3 format of the traversal
   * @static
   * @param {Traversal} traversal The traversal instance to convert.
   * @param {string} protocol The graph protocol to use. Supported protocols are 'graphson-2.0' and 'graphson-3.0'.
   * @returns {String}
   */
  static getQuery(traversal, protocol) {
    protocol = protocol || graphProtocol.graphson2;

    switch (protocol) {
      case graphProtocol.graphson2:
        return graphSON2Writer.write(traversal);
      case graphProtocol.graphson3:
        return graphSON3Writer.write(traversal);
    }

    throw new TypeError(`Protocol '${protocol}' not supported`);
  }
}

function getQueryWriter(writer) {
  return traversal => writer.write(traversal);
}

function getRowParser(reader) {
  return row => {
    const item = reader.read(JSON.parse(row['gremlin']));
    return { object: item['result'], bulk: item['bulk'] || 1 };
  };
}

function queryWriterFactory(protocol) {
  if (!protocol) {
    return defaultQueryWriter;
  }

  const handler = queryWriters.get(protocol);
  // Use GraphSON2 as default: DSE 5.1+ should support it
  return handler || defaultQueryWriter;
}

function rowParserFactory(protocol) {
  const handler = rowParsers.get(protocol);

  if (!handler) {
    // Avoid handling serialization for formats we don't recognize.
    // The driver will default to their own serialization logic.
    return null;
  }

  return handler;
}

module.exports = { DseRemoteConnection, queryWriterFactory, graphProtocol };
