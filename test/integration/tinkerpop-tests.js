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
var dseGraph = require('../../index');
var tinkerpop = dseGraph.tinkerpop;
var P = tinkerpop.process.P;
var __ = tinkerpop.process.statics;

vdescribe('5.0', 'DseGraph', function () {
  this.timeout(60000);
  before(helper.ccm.startAllTask(1, { workloads: ['graph','spark'] }));
  before(helper.createModernGraph('name1'));
  before(wrapClient(function (client, done) {
    // put schema into development mode so we can make changes on the fly.
    client.executeGraph("schema.config().option('graph.schema_mode').set('development')", done);
  }));
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
  describe('queryFromTraversal()', function() {
    it('should produce a query string usable with DseClient#executeGraph()', wrapClient(function (client, done) {
      var g = dseGraph.traversalSource();
      var query = dseGraph.queryFromTraversal(g.V().hasLabel('software').valueMap());
      client.executeGraph(query, {}, { executionProfile: 'traversal' }, function(err, result) {
        assert.ifError(err);
        var res = result.toArray();
        res.forEach(function(software) {
          assert.strictEqual(software['lang'][0], 'java');
        });
        var names = res.map(function(s) {
          return s['name'][0];
        }).sort();
        assert.deepEqual(names, ['lop', 'ripple']);
        done();
      });
    }));
  });
  describe('traversalSource()', function () {
    var schemaCounter = 0;
    it('should execute a simple traversal', wrapTraversal(function (g, done) {
      g.V().count().list(function(err, result) {
        assert.ifError(err);
        helper.assertInstanceOf(result, Array);
        assert.strictEqual(result.length, 1);
        var count = result[0];
        helper.assertInstanceOf(count, cassandra.types.Long);
        assert.ok(count.greaterThan(cassandra.types.Long.ZERO));
        done();
      });
    }));
    it('should use vertex id as parameter', wrapTraversal(function (g, done) {
      // given an existing vertex
      g.V().hasLabel("person").has("name", "marko").one(function (err, marko) {
        assert.ifError(err);

        // then should be able to retrieve the vertex's properties by id.
        g.V(marko.id).valueMap('name').one(function (err, v2) {
          assert.ifError(err);
          assert.strictEqual(v2.name[0], 'marko');
          done();
        });
      })
    }));
    it('should use edge id as parameter', wrapTraversal(function (g, done) {
      // given an existing edge
      g.E().has("weight", 0.2).one(function (err, created) {
        assert.ifError(err);

        // should be able to retrieve incoming vertex properties by edge id.
        g.E(created.id).inV().valueMap('name', 'lang').one(function (err, software) {
          assert.ifError(err);
          assert.strictEqual(software.name[0], 'lop');
          assert.strictEqual(software.lang[0], 'java');
          done();
        });
      });
    }));
    it('should be able to query with a predicate', wrapTraversal(function (g, done) {
      // Provide a predicate 'P.eq(0.2)' to ensure it is properly converted to graphson.
      // This module has a special case serializer for Predicates as DSE defines some
      // custom predicates for search and geo.
      g.E().has("weight", P.eq(0.2)).outV().valueMap('name').one(function (err, peter) {
        assert.ifError(err);
        assert.strictEqual(peter.name[0], 'peter');
        done();
      });
    }));
    it('should deserialize vertex id as map', wrapTraversal(function (g, done) {
      // given an existing vertex
      // then id should be a map with expected values.
      // Note: this is pretty dependent on DSE Graph's underlying id structure which may vary in the future.
      g.V().hasLabel('person').has('name', 'marko').one(function (err, result) {
        var id = result.id;
        // ~label, community_id, member_id
        assert.strictEqual(Object.keys(id).length, 3);
        assert.strictEqual(id['~label'], "person");
        assert.ok("community_id");
        assert.ok("member_id");
        done();
      });
    }));
    it('should handle result object of mixed data', wrapTraversal(function (g, done) {
      // find all software vertices and select name, language, and find all vertices that created such software.
      g.V().hasLabel("software")
        .as("a", "b", "c")
        .select("a", "b", "c")
        .by("name")
        .by("lang")
        .by(__.in_("created").valueMap('name').fold()).list(function(err, result) {
        assert.ifError(err);
        // ensure that lop and ripple and their data are the results returned.
        assert.strictEqual(result.length, 2);
        var software = result.map(function (entry) {
          return entry['a']
        }).sort();
        assert.deepEqual(software, ['lop', 'ripple']);
        result.forEach(function (entry) {
          // both software are written in java.
          assert.strictEqual(entry['b'], 'java');
          var people = entry['c'].map(function(e) {
            return e['name'][0];
          }).sort();
          if(entry['a'] === 'lop') {
            // lop, 'c' should contain marko, josh, peter.
            assert.deepEqual(people, ['josh', 'marko', 'peter']);
          } else {
            // only peter made ripple.
            assert.deepEqual(people, ['josh']);
          }
        });
        done();
      });
    }));
    it('should retrieve path with labels', wrapTraversal(function (g, done) {
      // find all path traversals for a person whom Marko knows that has created software and what
      // that software is.
      // The paths should be:
      // marko -> knows -> josh -> created -> lop
      // marko -> knows -> josh -> created -> ripple
      g.V().hasLabel('person').has('name', 'marko').as('a')
        .outE('knows').as('b')
        .inV().as('c', 'd')
        .outE('created').as('e', 'f', 'g')
        .inV().as('h')
        .path()
        .list(function(err, results) {
        assert.ifError(err);
        assert.ok(results);
        // There should only be two paths.
        assert.strictEqual(results.length, 2);
        results.forEach(function(path) {
          // ensure the labels are organized as requested.
          var labels = path.labels;
          assert.strictEqual(labels.length, 5);
          assert.deepEqual(labels, [['a'], ['b'], ['c', 'd'], ['e', 'f', 'g'], ['h']]);

          // ensure the returned path matches what was expected and that each object
          // has the expected contents.
          var objects = path.objects;
          assert.strictEqual(objects.length, 5);
          var marko = objects[0];
          var knows = objects[1];
          var josh = objects[2];
          var created = objects[3];
          var software = objects[4];

          // marko
          assert.strictEqual(marko.label, 'person');
          assert.strictEqual(marko.properties.name[0].value, 'marko');
          assert.strictEqual(marko.properties.age[0].value, 29);

          // knows
          assert.strictEqual(knows.label, 'knows');
          // todo: properties
          //assert.strictEqual(knows.properties.weight, 1);
          assert.strictEqual(knows.outVLabel, 'person');
          assert.deepEqual(knows.outV, marko.id);
          assert.strictEqual(knows.inVLabel, 'person');
          assert.deepEqual(knows.inV, josh.id);

          // josh
          assert.strictEqual(josh.label, 'person');
          assert.strictEqual(josh.properties.name[0].value, 'josh');
          assert.strictEqual(josh.properties.age[0].value, 32);

          // who created
          assert.strictEqual(created.label, 'created');
          assert.strictEqual(created.outVLabel, 'person');
          assert.deepEqual(created.outV, josh.id);
          assert.strictEqual(created.inVLabel, 'software');
          assert.deepEqual(created.inV, software.id);

          // software
          if(software.properties.name[0].value === 'lop') {
            // todo: properties
            //assert.strictEqual(created.properties.weight, 0.4);
          } else {
            //assert.strictEqual(created.properties.weight, 1.0);
            assert.strictEqual(software.properties.name[0].value, 'ripple');
          }

          assert.strictEqual(software.label, 'software');
          assert.strictEqual(software.properties.lang[0].value, 'java');
        });
        done();
      });
    }));
    it('should handle subgraph', wrapTraversal(function (g, done) {
      // retrieve a subgraph on the knows relationship, this omits the created edges.
      g.E().hasLabel('knows').subgraph('subGraph').cap('subGraph').one(function (err, graph){
        assert.ifError(err);
        // there should be only 2 edges (since there are only 2 knows relationships) and 3 vertices
        assert.strictEqual(graph['vertices'].length, 3);
        assert.strictEqual(graph['edges'].length, 2);
        done();
      })
    }));
    xit('should handle tree', wrapTraversal(function (g, done) {
      g.V().hasLabel("person").out("knows").out("created").tree().by("name").one(function (err, tree) {
        assert.ifError(err);
        assert.strictEqual(tree.length, 1);

        // root should be marko.
        var marko = tree[0];
        assert.strictEqual(marko['key'], 'marko');
        assert.strictEqual(marko['value'].length, 1);

        // who has one node, josh
        var josh = marko['value'][0];
        assert.strictEqual(josh['key'], 'josh');
        assert.strictEqual(josh['value'].length, 2);

        // who has two leafs, lop and ripple
        josh['value'].forEach(function(software) {
          assert.strictEqual(software['value'].length, 0);
        });
        var names = josh['value'].map(function(s) {
          return s['key'];
        }).sort();
        assert.deepEqual(names, ['lop', 'ripple']);
        done();
      });
    }));
    it('should execute traversal with enums', wrapTraversal(function (g, done) {
      var order = tinkerpop.process.order;
      g.V().hasLabel('person').has('age').order().by('age', order.decr).list(function (err, people) {
        assert.ifError(err);
        assert.strictEqual(people.length, 4);
        assert.deepEqual(people.map(function (p) {
          return p.properties['name'][0].value;
        }), ['peter', 'josh', 'marko', 'vadas']);
        done();
      });
    }));
    it('should be able to create and retrieve a vertex with a vertex property with meta properties', wrapClient(function (client, done) {
      // This currently doesn't work because VertexProperty doesn't support meta properties.
      var g = createTraversal(client);
      helper.series([
        function createSchema(next) {
          // given a schema that defines meta properties.
          client.executeGraph(helper.queries.graph.metaPropsSchema, next);
        },
        function createVertex(next) {
          // when adding a vertex with that meta property
          g.addV('meta_v').property('meta_prop', 'hello', 'sub_prop', 'hi', 'sub_prop2', 'hi2').one(function (err, v) {
            assert.ifError(err);
            // then the created vertex should have the meta prop present with its sub properties.
            var meta_prop = v.properties['meta_prop'][0];
            assert.strictEqual(meta_prop.label, 'meta_prop');
            assert.strictEqual(meta_prop.key, 'meta_prop');
            assert.strictEqual(meta_prop.value, 'hello');
            // TODO: Parse subproperties.
            next();
          });
        }
      ], done);
    }));
    it('should be able create and retrieve a vertex with a vertex property with multi-cardinality', wrapClient(function (client, done) {
      var g = createTraversal(client);
      helper.series([
        function createSchema(next) {
          // given a schema that defines multiple cardinality properties.
          client.executeGraph(helper.queries.graph.multiCardinalitySchema, next);
        },
        function createVertex(next) {
          // when adding a vertex with a multiple cardinality property
          g.addV("multi_v")
            .property("multi_prop", "Hello")
            .property("multi_prop", "Sweet")
            .property("multi_prop", "World").one(function (err, v) {
            // then the created vertex should have the multi-cardinality property present with its values.
            assert.ifError(err);
            var multi_props = v.properties['multi_prop'];
            var values = multi_props.map(function(e) {
              return e.value;
            }).sort();
            assert.deepEqual(values, ['Hello', 'Sweet', 'World']);
            next();
          });
        }
      ], done);
    }));
    [
      // Validate that all supported property types by DSE graph are properly encoded / decoded.
      ['Boolean', [true, false]],
      ['Int', [2147483647, -2147483648, 0, 42]],
      ['Smallint', [-32768, 32767, 0, 42]],
      ['Bigint', [Long.fromString('-9007199254740991'), Long.ZERO, Long.fromString('1234')]],
      ['Float', [3.1415927]],
      ['Double', [Math.PI]],
      ['Decimal', [cassandra.types.BigDecimal.fromString("8675309.9998")]],
      ['Varint', [cassandra.types.Integer.fromString("8675309")]],
      ['Timestamp', [new Date('2016-02-04T02:26:31.657Z'), new Date('2016-01-01T09:30:39.523Z')]],
      ['Blob', [new Buffer('Hello world!', 'utf8')]],
      ['Text', ["", "75", "Lorem Ipsum"]],
      ['Uuid', [Uuid.random()]],
      ['Inet', [InetAddress.fromString("127.0.0.1"), InetAddress.fromString("::1"),
        InetAddress.fromString("2001:db8:85a3:0:0:8a2e:370:7334")]],
      ['Point', [new Point(0, 1), new Point(-5, 20)]],
      ['Linestring', [new LineString(new Point(30, 10), new Point(10, 30), new Point(40, 40))]],
      ['Polygon', [new Polygon(
        [new Point(35, 10), new Point(45, 45), new Point(15, 40), new Point(10, 20), new Point(35, 10)],
        [new Point(20, 30), new Point(35, 35), new Point(30, 20), new Point(20, 30)]
      )]]
    ].forEach(function (args) {
      var id = schemaCounter++;
      var propType = args[0];
      var input = args[1];
      it(util.format('should create and retrieve vertex with property of type %s', propType), wrapTraversal(function(g, done) {
        var vertexLabel = "vertex" + id;
        var propertyName = "prop" + id;

        helper.series([
          function addVertex(next) {
            helper.timesSeries(input.length, function(index, timesNext) {
              var value = input[index];
              // Add vertex and ensure it is properly decoded.
              g.addV(vertexLabel).property(propertyName, value).list(function (err, result) {
                assert.ifError(err);
                validateVertexResult(result, input[index], vertexLabel, propertyName);
                // Ensure the vertex is retrievable.
                g.V().hasLabel(vertexLabel).has(propertyName, value).list(function (err, result) {
                  assert.ifError(err);
                  validateVertexResult(result, input[index], vertexLabel, propertyName);
                  timesNext();
                })
              });
            }, next);
          }
        ], done);
      }));
    });
    describe('Traversal#list()', function () {
      it('should return an Array of traversers', wrapTraversal(function (g, done) {
        g.V().hasLabel('person').list(function (err, people) {
          assert.ifError(err);
          helper.assertInstanceOf(people, Array);
          people.forEach(function eachPerson(v) {
            helper.assertInstanceOf(v, dse.graph.Vertex);
            assert.ok(v.properties['name'][0].value);
          });
          done();
        });
      }));
      it('should return an empty array of traversers when there is no match', wrapTraversal(function (g, done) {
        g.V().hasLabel('notFound').list(function (err, result) {
          assert.ifError(err);
          helper.assertInstanceOf(result, Array);
          assert.strictEqual(result.length, 0);
          done();
        });
      }));
    });
    describe('Traversal#one()', function () {
      it('should retrieve a single traverser', wrapTraversal(function (g, done) {
        g.V().hasLabel('person').one(function (err, person) {
          assert.ifError(err);
          helper.assertInstanceOf(person, dse.graph.Vertex);
          assert.ok(person.properties['name'][0].value);
          done();
        });
      }));
      it('should return an null traverser when there is no match', wrapTraversal(function (g, done) {
        g.V().hasLabel('notFound').one(function (err, result) {
          assert.ifError(err);
          assert.strictEqual(result, null);
          done();
        });
      }));
    });
    describe('createExecutionProfile()', function() {
      var opts = helper.getOptions(helper.extend({}, {
        graphOptions: {name: 'name2'},
        profiles: [
          dseGraph.createExecutionProfile('analytics', {graphOptions: {source: 'a'}})
        ]
      }));

      var client = new Client(opts);

      before(helper.createModernGraph('name2'));
      after(client.shutdown.bind(client));

      // Get vertex clusters of people.  We expect there to be 2 clusters:
      // 1) A cluster with only Peter, he doesn't have a 'knows' relation to anybody.
      // 2) A cluster of vertices around Marko, he knows Vadas and Josh.
      // The peerPressure step requires a OLAP GraphComputer, thus this should fail without the
      // analytics traversal source.
      function executeAnalyticsQueries(traversal, expectPass) {
        var pass = expectPass === undefined ? true : expectPass;
        return function (done) {
          traversal.V().hasLabel("person")
            .peerPressure().by("cluster")
            .group().by("cluster").by("name")
            .one(function (err, result) {
              if (pass) {
                assert.ifError(err);
                var clusters = Object.keys(result).map(function(k) {
                  return result[k].sort();
                }).sort(function(a, b) {
                  return a.length - b.length;
                });
                assert.deepEqual(clusters, [ ['peter'], ['josh', 'marko', 'vadas']]);
              } else {
                assert.ok(err);
              }
              done();
            });
        }
      }

      var a0 = createTraversal(client, {executionProfile: 'analytics'});
      var a1 = createTraversal(client, {graphSource: 'a'});
      var g = createTraversal(client);

      // should succeed since using 'a' source.
      it('should make an OLAP query using \'a\' traversal source', executeAnalyticsQueries(a0));
      // should succeed since using profile which specifies 'a' source.
      it('should make an OLAP query using profile with \'a\' traversal source', executeAnalyticsQueries(a1));
      // should fail since graph computer required.
      it('should make fail to make an OLAP query when using \'g\' traversal source', executeAnalyticsQueries(g, false));
    });
  });
});

/**
 * @param {Client} client
 * @param {GraphQueryOptions} [options]
 * @returns {GraphTraversalSource}
 */
function createTraversal(client, options) {
  return dseGraph.traversalSource(client, options);
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

function wrapTraversal(handler, options) {
  return wrapClient(function (client, next) {
    var g = createTraversal(client);
    handler(g, next);
  }, options);
}

function wrapClient(handler, options) {
  return (function wrappedTestCase(done) {
    var opts = helper.getOptions(helper.extend(options || {}, {
      graphOptions : { name: 'name1' },
      profiles: [
        dseGraph.createExecutionProfile('traversal', {})
      ]
    }));
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


