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

const util = require('util');
const { datastax } = require('cassandra-driver');
const glv = require('gremlin');
const { getCustomTypeSerializers } = datastax.graph;
const geo = require('./predicates/geo');
const search = require('./predicates/search');
const { TraversalBatch } = require('./types');

const graphSONValueKey = '@value';
const graphSONTypeKey = '@type';

/**
 * Uses toString() instance method and fromString() static method to serialize and deserialize the value.
 * @param {String} key
 * @param {Function} targetType
 * @constructor
 * @abstract
 */
function StringBasedSerializer(key, targetType) {
  if (!key) {
    throw new Error('Serializer must provide a type key');
  }
  if (!targetType) {
    throw new Error('Serializer must provide a target type');
  }
  this.key = key;
  this.targetType = targetType;
}

StringBasedSerializer.prototype.serialize = function (item) {
  const result = {};
  result[graphSONTypeKey] = this.key;
  result[graphSONValueKey] = item.toString();
  return result;
};

StringBasedSerializer.prototype.deserialize = function (obj) {
  let value = obj[graphSONValueKey];
  if (typeof value !== 'string') {
    value = value.toString();
  }
  return this.targetType.fromString(value);
};

StringBasedSerializer.prototype.canBeUsedFor = function (value) {
  return (value instanceof this.targetType);
};

function DistanceSerializer() {
  StringBasedSerializer.call(this, 'dse:Distance', geo.Distance);
}

util.inherits(DistanceSerializer, StringBasedSerializer);

function DsePSerializer() {
  this.key = 'dse:P';
}

/** @param {P} item */
DsePSerializer.prototype.serialize = function (item) {
  if (!this.writer) {
    throw new Error('Serializer writer is not defined');
  }
  const result = {};
  const resultValue = result[graphSONValueKey] = {
    'predicate': item.operator
  };
  if (item instanceof search.TextDistanceP) {
    result[graphSONTypeKey] = 'g:P';
    resultValue['value'] = {
      'query': item.value,
      'distance': item.distance
    };
    return result;
  }
  result[graphSONTypeKey] = 'dse:P';
  resultValue['predicateType'] = item instanceof geo.GeoP ? 'Geo' : 'P';
  if (item.other === undefined || item.other === null) {
    resultValue['value'] = this.writer.adaptObject(item.value);
  }
  else {
    resultValue['value'] = [ this.writer.adaptObject(item.value), this.writer.adaptObject(item.other) ];
  }
  return result;
};

DsePSerializer.prototype.canBeUsedFor = function (value) {
  return (value instanceof glv.process.P) &&
    // within() and without() have a specific syntax
    value.operator !== 'within' && value.operator !== 'without';
};

class TraversalBatchSerializer {
  constructor() {
    // Use a fixed name that doesn't conflict with TinkerPop and DS Graph
    this.key = 'client:batch';
  }

  serialize(batch) {
    return this.writer.adaptObject(batch.items);
  }

  canBeUsedFor(value) {
    return value instanceof TraversalBatch;
  }
}

function getGlvSerializers() {
  const serializersToOverride = {};
  [
    DistanceSerializer,
    DsePSerializer,
    TraversalBatchSerializer
  ].forEach(function append(serializerConstructor) {
    const instance = new serializerConstructor();
    serializersToOverride[instance.key] = instance;
  });

  const serializers = Object.assign({}, getCustomTypeSerializers(), serializersToOverride);

  // Base driver has an specific workaround for the difference between TinkerPop's Edge and driver's Edge.
  // Remove to default to TinkerPop structure elements.
  delete serializers['g:Edge'];
  return serializers;
}

exports.getGlvSerializers = getGlvSerializers;