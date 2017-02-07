/**
 * Copyright (C) 2016 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';

var util = require('util');
var dse = require('dse-driver');
var types = dse.types;
var glv = require('./tinkerpop');
var Point = dse.geometry.Point;
var LineString = dse.geometry.LineString;
var Polygon = dse.geometry.Polygon;
var geo = require('./predicates/geo');
var search = require('./predicates/search');

var graphSONValueKey = '@value';
var graphSONTypeKey = '@type';

function getGlvSerializers() {
  var serializers = {};
  [
    UuidSerializer,
    InstantSerializer,
    LongSerializer,
    BigDecimalSerializer,
    BigIntegerSerializer,
    InetAddressSerializer,
    BlobSerializer,
    PointSerializer,
    LineStringSerializer,
    PolygonSerializer,
    DistanceSerializer,
    DsePSerializer
  ].forEach(function append(serializerConstructor) {
    var instance = new serializerConstructor();
    serializers[instance.key] = instance;
  });
  return serializers;
}

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
  var result = {};
  result[graphSONTypeKey] = this.key;
  result[graphSONValueKey] = item.toString();
  return result;
};

StringBasedSerializer.prototype.deserialize = function (obj) {
  var value = obj[graphSONValueKey];
  if (typeof value !== 'string') {
    value = value.toString();
  }
  return this.targetType.fromString(value);
};

StringBasedSerializer.prototype.canBeUsedFor = function (value) {
  return (value instanceof this.targetType);
};

function UuidSerializer() {
  StringBasedSerializer.call(this, 'g:UUID', types.Uuid);
}

util.inherits(UuidSerializer, StringBasedSerializer);

function LongSerializer() {
  StringBasedSerializer.call(this, 'g:Int64', types.Long);
}

util.inherits(LongSerializer, StringBasedSerializer);

function BigDecimalSerializer() {
  StringBasedSerializer.call(this, 'gx:BigDecimal', types.BigDecimal);
}

util.inherits(BigDecimalSerializer, StringBasedSerializer);

function BigIntegerSerializer() {
  StringBasedSerializer.call(this, 'gx:BigInteger', types.Integer);
}

util.inherits(BigIntegerSerializer, StringBasedSerializer);

function InetAddressSerializer() {
  StringBasedSerializer.call(this, 'gx:InetAddress', types.InetAddress);
}

util.inherits(InetAddressSerializer, StringBasedSerializer);

function InstantSerializer() {
  StringBasedSerializer.call(this, 'gx:Instant', Date);
}

util.inherits(InstantSerializer, StringBasedSerializer);

InstantSerializer.prototype.serialize = function (item) {
  var result = {};
  result[graphSONTypeKey] = this.key;
  result[graphSONValueKey] = item.toISOString();
  return result;
};

InstantSerializer.prototype.deserialize = function (obj) {
  return new Date(obj[graphSONValueKey]);
};

function BlobSerializer() {
  StringBasedSerializer.call(this, 'dse:Blob', Buffer);
}

util.inherits(BlobSerializer, StringBasedSerializer);

BlobSerializer.prototype.serialize = function (item) {
  var result = {};
  result[graphSONTypeKey] = this.key;
  result[graphSONValueKey] = item.toString('base64');
  return result;
};

BlobSerializer.prototype.deserialize = function (obj) {
  return new Buffer(obj[graphSONValueKey], 'base64');
};

function PointSerializer() {
  StringBasedSerializer.call(this, 'dse:Point', Point);
}

util.inherits(PointSerializer, StringBasedSerializer);

function LineStringSerializer() {
  StringBasedSerializer.call(this, 'dse:LineString', LineString);
}

util.inherits(LineStringSerializer, StringBasedSerializer);

function PolygonSerializer() {
  StringBasedSerializer.call(this, 'dse:Polygon', Polygon);
}

util.inherits(PolygonSerializer, StringBasedSerializer);

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
  var result = {};
  var resultValue = result[graphSONValueKey] = {
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
  if (item.other == undefined) {
    resultValue['value'] = this.writer.adaptObject(item.value);
  }
  else {
    resultValue['value'] = [ this.writer.adaptObject(item.value), this.writer.adaptObject(item.other) ];
  }
  return result;
};

DsePSerializer.prototype.canBeUsedFor = function (value) {
  return (value instanceof glv.process.P);
};

exports.getGlvSerializers = getGlvSerializers;