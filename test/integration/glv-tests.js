/**
 * Copyright (C) 2016 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
var util = require('util');
var assert = require('assert');
var dse = require('dse-driver');
var Client = dse.Client;
var helper = require('../helper');
var vdescribe = helper.vdescribe;
var cassandra = require('cassandra-driver');
var InetAddress = cassandra.types.InetAddress;
var Uuid = cassandra.types.Uuid;
var Long = cassandra.types.Long;
var geo = dse.geometry;
var Point = geo.Point;
var LineString = geo.LineString;
var Polygon = geo.Polygon;
var glv = require('../../lib/tinkerpop');
var graphTraversalSource = require('../../index').traversalSource;

vdescribe('5.0', 'GLV', function () {
  this.timeout(60000);
  before(helper.ccm.startAllTask(1, { workloads: ['graph'] }));
  before(helper.createModernGraph);
  after(helper.ccm.remove.bind(helper.ccm));
  describe('Client#executeGraph()', function () {
    it('should execute a GraphSON query', wrapClient(function (client, done) {
      var queries = [
        '{"@type": "g:Bytecode", "@value": {"step": [["V"]]}}',
        '{"@type": "g:Bytecode", "@value": {"step": [["V"],["count"]]}}'
      ];
      helper.eachSeries(queries, function (query, next) {
        client.executeGraph(query, null, { graphName: 'name1', graphLanguage: 'bytecode-json'}, function (err, result) {
          assert.ifError(err);
          assert.ok(result);
          next();
        });
      }, done);
    }));
  });
  describe('execution', function () {
    var schemaCounter = 0;
    it('should execute a simple traversal', wrapClient(function (client, done) {
      var g = createTraversal(client);
      execute(g.V().count(), function (err, result) {
        assert.ifError(err);
        helper.assertInstanceOf(result, Array);
        assert.strictEqual(result.length, 1);
        var count = result[0];
        helper.assertInstanceOf(count, cassandra.types.Long);
        assert.ok(count.greaterThan(cassandra.types.Long.ZERO));
        done();
      });
    }));
    it('should execute traversal with enums', wrapClient(function (client, done) {
      var g = createTraversal(client);
      var order = glv.process.order;
      execute(g.V().hasLabel('person').has('age').order().by('age', order.decr), function (err, people) {
        assert.ifError(err);
        assert.strictEqual(people.length, 4);
        assert.deepEqual(people.map(function (p) {
          return p.properties['name'][0].value;
        }), ['peter', 'josh', 'marko', 'vadas']);
        done();
      });
    }));
    [
      // Validate that all supported property types by DSE graph are properly encoded / decoded.
      ['Boolean()', [true, false]],
      ['Int()', [2147483647, -2147483648, 0, 42]],
      ['Smallint()', [-32768, 32767, 0, 42]],
      ['Bigint()', [-9007199254740991, 0, Long.fromString('1234')],
        [Long.fromString('-9007199254740991'), Long.ZERO, cassandra.types.Long.fromString('1234')]],
      ['Float()', [3.1415927]],
      ['Double()', [Math.PI]],
      ['Decimal()', [cassandra.types.BigDecimal.fromString("8675309.9998")]],
      ['Varint()', [cassandra.types.Integer.fromString("8675309")]],
      ['Timestamp()', ['2016-02-04T02:26:31.657Z', new Date('2016-01-01T09:30:39.523Z')],
        [new Date('2016-02-04T02:26:31.657Z'), new Date('2016-01-01T09:30:39.523Z')]],
      ['Blob()', [new Buffer('Hello world!', 'utf8')]],
      ['Text()', ["", "75", "Lorem Ipsum"]],
      ['Uuid()', [Uuid.random()]],
      ['Inet()', [InetAddress.fromString("127.0.0.1"), InetAddress.fromString("::1"),
        InetAddress.fromString("2001:db8:85a3:0:0:8a2e:370:7334")]],
      ['Point()', [new Point(0, 1), new Point(-5, 20)]],
      ['Linestring()', [new LineString(new Point(30, 10), new Point(10, 30), new Point(40, 40))]],
      ['Polygon()', [new Polygon(
        [new Point(35, 10), new Point(45, 45), new Point(15, 40), new Point(10, 20), new Point(35, 10)],
        [new Point(20, 30), new Point(35, 35), new Point(30, 20), new Point(20, 30)]
      )]]
    ].forEach(function (args) {
      var id = schemaCounter++;
      var propType = args[0];
      var input = args[1];
      var expected = args.length >= 3 ? args[2] : input;
      it(util.format('should create and retrieve vertex with property of type %s', propType), wrapClient(function(client, done) {
        var vertexLabel = "vertex" + id;
        var propertyName = "prop" + id;
        var schemaQuery = '' +
          'schema.propertyKey(propertyName).' + propType + '.create()\n' +
          'schema.vertexLabel(vertexLabel).properties(propertyName).create()';

        helper.series([
          function createSchema(next) {
            client.executeGraph(schemaQuery, {vertexLabel: vertexLabel, propertyName: propertyName}, null, next);
          },
          function addVertex(next) {
            helper.timesSeries(input.length, function(index, timesNext) {
              var value = input[index];
              var g = createTraversal(client);
              // Add vertex and ensure it is properly decoded.
              execute(g.addV(vertexLabel).property(propertyName, value), function (err, result) {
                assert.ifError(err);
                validateVertexResult(result, expected[index], vertexLabel, propertyName);
                // Ensure the vertex is retrievable.
                var g = createTraversal(client);
                execute(g.V().hasLabel(vertexLabel).has(propertyName, value), function (err, result) {
                  assert.ifError(err);
                  validateVertexResult(result, expected[index], vertexLabel, propertyName);
                  timesNext();
                })
              });
            }, next);
          }
        ], done);
      }));
    });
  });
  describe('Traversal#list()', function () {
    it('should return an Array of traversers', wrapClient(function (client, done) {
      var g = createTraversal(client);
      g.V().hasLabel('person').list(function (err, people) {
        assert.ifError(err);
        helper.assertInstanceOf(people, Array);
        people.forEach(function eachPerson(v) {
          helper.assertInstanceOf(v, glv.structure.Vertex);
          assert.ok(v.properties['name'][0].value);
        });
        done();
      });
    }));
    it('should return an empty array of traversers when there is no match', wrapClient(function (client, done) {
      var g = createTraversal(client);
      g.V().hasLabel('notFound').list(function (err, result) {
        assert.ifError(err);
        helper.assertInstanceOf(result, Array);
        assert.strictEqual(result.length, 0);
        done();
      });
    }));
  });
  describe('Traversal#one()', function () {
    it('should retrieve a single traverser', wrapClient(function (client, done) {
      var g = createTraversal(client);
      g.V().hasLabel('person').one(function (err, person) {
        assert.ifError(err);
        helper.assertInstanceOf(person, glv.structure.Vertex);
        assert.ok(person.properties['name'][0].value);
        done();
      });
    }));
    it('should return an null traverser when there is no match', wrapClient(function (client, done) {
      var g = createTraversal(client);
      g.V().hasLabel('notFound').one(function (err, result) {
        assert.ifError(err);
        assert.strictEqual(result, null);
        done();
      });
    }));
  });
});

/**
 * @param {Client} client
 * @param {GraphQueryOptions} [options]
 * @returns {GraphTraversalSource}
 */
function createTraversal(client, options) {
  return graphTraversalSource(client, options);
}

/**
 * @param {GraphTraversal} traversal
 * @param {Function} callback
 */
function execute(traversal, callback) {
  traversal.list(callback);
}

function validateVertexResult(result, expectedResult, vertexLabel, propertyName) {
  assert.strictEqual(result.length, 1);
  var vertex = result[0];
  assert.equal(vertex.label, vertexLabel);
  var propValue = vertex.properties[propertyName][0].value;
  if (typeof expectedResult === 'object') {
    helper.assertInstanceOf(propValue, expectedResult.constructor);
  }
  if (expectedResult.equals) {
    assert.ok(expectedResult.equals(propValue), '!equals(' + expectedResult + ', ' + propValue + ')');
    return;
  }
  if ((expectedResult instanceof Date) || (expectedResult instanceof Buffer)) {
    assert.deepEqual(expectedResult, propValue);
    return;
  }
  assert.strictEqual(propValue, expectedResult);
}

function wrapClient(handler, options) {
  return (function wrappedTestCase(done) {
    var opts = helper.getOptions(helper.extend(options || {}, { graphOptions : { name: 'name1' } }));
    var client = new Client(opts);
    helper.series([
      client.connect.bind(client),
      function testItem(next) {
        handler(client, next);
      }
    ], function seriesFinished(err) {
      // Shutdown regardless of the result
      client.shutdown(function shutdownCallback() {
        done(err);
      });
    })
  });
}


