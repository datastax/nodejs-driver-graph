/**
 * Copyright DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';

const util = require('util');
const assert = require('assert');
const dse = require('dse-driver');
const Client = dse.Client;
const helper = require('../helper');
const vdescribe = helper.vdescribe;
const types = dse.types;
const InetAddress = types.InetAddress;
const Uuid = types.Uuid;
const Long = types.Long;
const geometry = dse.geometry;
const Point = geometry.Point;
const LineString = geometry.LineString;
const Polygon = geometry.Polygon;
const dseGraph = require('../../index');
const tinkerpop = require('gremlin');
const P = tinkerpop.process.P;
const __ = tinkerpop.process.statics;
const wrapTraversal = helper.wrapTraversal;
const wrapClient = helper.wrapClient;
const wrapClientPromise = helper.wrapClientPromise;

vdescribe('5.0', 'DseGraph', function () {
  this.timeout(240000);

  before(helper.ccm.startAllTask(1, { workloads: ['graph','spark'] }));
  before(helper.createModernGraph('name1'));
  before(wrapClient(function (client, done) {
    // put schema into development mode so we can make changes on the fly.
    client.executeGraph("schema.config().option('graph.schema_mode').set('development')", done);
  }));
  after(helper.ccm.remove.bind(helper.ccm));

  describe('Client#executeGraph()', function () {
    it('should execute a GraphSON query', wrapClient(function (client, done) {
      const queries = [
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
      const g = dseGraph.traversalSource();
      const query = dseGraph.queryFromTraversal(g.V().hasLabel('software').valueMap());
      client.executeGraph(query, {}, { executionProfile: 'traversal' }, function(err, result) {
        assert.ifError(err);
        const res = result.toArray();
        res.forEach(function(software) {
          assert.strictEqual(software['lang'][0], 'java');
        });
        const names = res.map(function(s) {
          return s['name'][0];
        }).sort();
        assert.deepEqual(names, ['lop', 'ripple']);
        done();
      });
    }));
  });

  describe('traversalSource()', function () {
    let schemaCounter = 0;
    const client = new Client(helper.getOptions({ graphOptions : { name: 'name1' } }));
    const g = dseGraph.traversalSource(client);

    before(() => client.connect());
    after(() => client.shutdown());

    it('should execute a simple traversal', function () {
      return g.V().count().toList().then(result => {
        helper.assertInstanceOf(result, Array);
        assert.strictEqual(result.length, 1);
        const count = result[0];
        helper.assertInstanceOf(count, types.Long);
        assert.ok(count.greaterThan(types.Long.ZERO));
      });
    });

    it('should throw TypeError to execute invalid traversal method', function () {
      assert.throws(() => g.toList(), TypeError);
    });

    it('should use vertex id as parameter', function () {
      // given an existing vertex
      return g.V().hasLabel("person").has("name", "marko").toList()
        .then(list => {
          const marko = getFirst(list);
          return g.V(marko.id).valueMap('name').toList();
        })
        .then(list => {
          const v2 = getFirst(list);
          assert.strictEqual(v2.name[0], 'marko');
        });
    });

    it('should use edge id as parameter', function () {
      // given an existing edge
      return g.E().has("weight", 0.2).toList()
        .then(function (list) {
          const created = getFirst(list);
          // should be able to retrieve incoming vertex properties by edge id.
          return g.E(created.id).inV().valueMap('name', 'lang').toList();
        })
        .then(list => {
          const software = getFirst(list);
          assert.strictEqual(software.name[0], 'lop');
          assert.strictEqual(software.lang[0], 'java');
        });
    });

    it('should be able to query with a predicate', function () {
      // Provide a predicate 'P.eq(0.2)' to ensure it is properly converted to graphson.
      // This module has a special case serializer for Predicates as DSE defines some
      // custom predicates for search and geo.
      return g.E().has("weight", P.eq(0.2)).outV().valueMap('name').toList()
        .then(list => {
          const peter = getFirst(list);
          assert.strictEqual(peter.name[0], 'peter');
          //testing max and min values
          return g.E().has("weight", P.lt(-9007199254740991)).outV().valueMap('name').toList();
        })
        .then(result => {
          assert.strictEqual(result.length, 0);
          //testing max and min values
          return g.E().has("weight", P.gt(9007199254740991)).outV().valueMap('name').toList();
        })
        .then(result => {
          assert.strictEqual(result.length, 0);
        });
    });

    it('should deserialize vertex id as map', function () {
      // given an existing vertex
      // then id should be a map with expected values.
      // Note: this is pretty dependent on DSE Graph's underlying id structure which may vary in the future.
      return g.V().hasLabel('person').has('name', 'marko').toList().then(list => {
        const id = getFirst(list).id;
        // ~label, community_id, member_id
        assert.strictEqual(Object.keys(id).length, 3);
        assert.strictEqual(id['~label'], "person");
        assert.ok("community_id");
        assert.ok("member_id");
      });
    });

    it('should handle result object of mixed data', function () {
      // find all software vertices and select name, language, and find all vertices that created such software.
      return g.V().hasLabel("software")
        .as("a", "b", "c")
        .select("a", "b", "c")
        .by("name")
        .by("lang")
        .by(__.in_("created").valueMap('name').fold()).toList().then(result => {
          // ensure that lop and ripple and their data are the results returned.
          assert.strictEqual(result.length, 2);
          const software = result.map(function (entry) {
            return entry['a'];
          }).sort();
          assert.deepEqual(software, ['lop', 'ripple']);
          result.forEach(function (entry) {
            // both software are written in java.
            assert.strictEqual(entry['b'], 'java');
            const people = entry['c'].map(function(e) {
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
        });
    });

    it('should retrieve path with labels', function () {
      // find all path traversals for a person whom Marko knows that has created software and what
      // that software is.
      // The paths should be:
      // marko -> knows -> josh -> created -> lop
      // marko -> knows -> josh -> created -> ripple
      return g.V().hasLabel('person').has('name', 'marko').as('a')
        .outE('knows').as('b')
        .inV().as('c', 'd')
        .outE('created').as('e', 'f', 'g')
        .inV().as('h')
        .path()
        .toList().then(results => {
          assert.ok(results);
          // There should only be two paths.
          assert.strictEqual(results.length, 2);
          results.forEach(path => {
          // ensure the labels are organized as requested.
            const labels = path.labels;
            assert.strictEqual(labels.length, 5);
            assert.deepEqual(labels, [['a'], ['b'], ['c', 'd'], ['e', 'f', 'g'], ['h']]);

            // ensure the returned path matches what was expected and that each object
            // has the expected contents.
            const objects = path.objects;
            assert.strictEqual(objects.length, 5);
            const marko = objects[0];
            const knows = objects[1];
            const josh = objects[2];
            const created = objects[3];
            const software = objects[4];

            // marko
            assert.strictEqual(marko.label, 'person');

            // knows
            assert.strictEqual(knows.label, 'knows');
            assert.strictEqual(knows.outVLabel, 'person');
            assert.deepEqual(knows.outV, marko.id);
            assert.strictEqual(knows.inVLabel, 'person');
            assert.deepEqual(knows.inV, josh.id);

            // josh
            assert.strictEqual(josh.label, 'person');

            // who created
            assert.strictEqual(created.label, 'created');
            assert.strictEqual(created.outVLabel, 'person');
            assert.deepEqual(created.outV, josh.id);
            assert.strictEqual(created.inVLabel, 'software');
            assert.deepEqual(created.inV, software.id);

            // software
            assert.strictEqual(software.label, 'software');
          });
        });
    });

    it('should handle subgraph', function () {
      // retrieve a subgraph on the knows relationship, this omits the created edges.
      return g.E().hasLabel('knows').subgraph('subGraph').cap('subGraph').toList().then(list => {
        const graph = getFirst(list);
        // there should be only 2 edges (since there are only 2 knows relationships) and 3 vertices
        assert.strictEqual(graph['vertices'].length, 3);
        assert.strictEqual(graph['edges'].length, 2);
      });
    });

    xit('should handle tree', function () {
      return g.V().hasLabel("person").out("knows").out("created").tree().by("name").toList().then(list => {
        const tree = getFirst(list);
        assert.strictEqual(tree.length, 1);

        // root should be marko.
        const marko = tree[0];
        assert.strictEqual(marko['key'], 'marko');
        assert.strictEqual(marko['value'].length, 1);

        // who has one node, josh
        const josh = marko['value'][0];
        assert.strictEqual(josh['key'], 'josh');
        assert.strictEqual(josh['value'].length, 2);

        // who has two leafs, lop and ripple
        josh['value'].forEach(function(software) {
          assert.strictEqual(software['value'].length, 0);
        });
        const names = josh['value'].map(s => s['key']).sort();
        assert.deepEqual(names, ['lop', 'ripple']);
      });
    });

    it('should execute traversal with enums', function () {
      const order = tinkerpop.process.order;
      return g.V().hasLabel('person').has('age').order().by('age', order.decr).toList().then(people => {
        assert.strictEqual(people.length, 4);
        assert.deepEqual(people.map(p => p.properties['name'][0].value), ['peter', 'josh', 'marko', 'vadas']);
      });
    });

    it('should be able to create and retrieve a vertex with a vertex property with meta properties', function () {
      // This currently doesn't work because VertexProperty doesn't support meta properties.
      const g = createTraversal(client);
      return client.executeGraph(helper.queries.graph.metaPropsSchema)
        .then(() => g.addV('meta_v').property('meta_prop', 'hello', 'sub_prop', 'hi', 'sub_prop2', 'hi2').toList())
        .then(list => {
          const v = getFirst(list);
          // then the created vertex should have the meta prop present with its sub properties.
          const meta_prop = v.properties['meta_prop'][0];
          helper.assertInstanceOf(meta_prop, dse.graph.VertexProperty);
          assert.strictEqual(meta_prop.label, 'meta_prop');
          assert.strictEqual(meta_prop.key, 'meta_prop');
          assert.strictEqual(meta_prop.value, 'hello');
          // sub properties should be present and have the same values as those inserted.
          assert.deepEqual(meta_prop.properties, { 'sub_prop' : 'hi', 'sub_prop2' : 'hi2' });
        });
    });

    it('should be able create and retrieve a vertex with a vertex property with multi-cardinality', function () {
      const g = createTraversal(client);
      return client.executeGraph(helper.queries.graph.multiCardinalitySchema)
        .then(() => g.addV("multi_v").property("multi_prop", "Hello")
          .property("multi_prop", "Sweet").property("multi_prop", "World").toList())
        .then(list => {
          const v = getFirst(list);
          const multi_props = v.properties['multi_prop'];
          const values = multi_props.map(function(e) {
            return e.value;
          }).sort();
          assert.deepEqual(values, ['Hello', 'Sweet', 'World']);
        });
    });

    const is51 = helper.isDseGreaterThan('5.1');
    const values = [
      // Validate that all supported property types by DSE graph are properly encoded / decoded.
      ['Boolean', [true, false]],
      ['Int', [2147483647, -2147483648, 0, 42]],
      ['Smallint', [-32768, 32767, 0, 42]],
      ['Bigint', [Long.fromString('-9007199254740991'), Long.ZERO, Long.fromString('1234')]],
      ['Float', [3.1415927]],
      ['Double', [Math.PI]],
      ['Decimal', [types.BigDecimal.fromString("8675309.9998")]],
      ['Varint', [types.Integer.fromString("8675309")]],
      ['Timestamp', [new Date('2016-02-04T02:26:31.657Z'), new Date('2016-01-01T09:30:39.523Z')]],
      ['Blob', [Buffer.from('Hello world!', 'utf8')]],
      ['Text', ["", "75", "Lorem Ipsum"]],
      ['Uuid', [Uuid.random()]],
      ['Inet', [InetAddress.fromString("127.0.0.1"), InetAddress.fromString("::1"),
        InetAddress.fromString("2001:db8:85a3:0:0:8a2e:370:7334")]],
      ['Point', [new Point(0, 1), new Point(-5, 20)]],
      ['Linestring', [new LineString(new Point(30, 10), new Point(10, 30), new Point(40, 40))]],
      ['Polygon', [new Polygon(
        [new Point(35, 10), new Point(45, 45), new Point(15, 40), new Point(10, 20), new Point(35, 10)],
        [new Point(20, 30), new Point(35, 35), new Point(30, 20), new Point(20, 30)]
      )]]];

    if (is51) {
      values.push.apply(values, [
        ['Date()', [ new types.LocalDate(2017, 2, 3), new types.LocalDate(-5, 2, 8) ]],
        ['Time()', [ types.LocalTime.fromString('4:53:03.000000021') ]]
      ]);
    }

    values.forEach(function (args) {
      const id = schemaCounter++;
      const propType = args[0];
      const input = args[1];
      it(util.format('should create and retrieve vertex with property of type %s', propType), wrapTraversal(function(g, done) {
        const vertexLabel = "vertex" + id;
        const propertyName = "prop" + id;

        helper.timesSeries(input.length, function(index, timesNext) {
          const value = input[index];
          // Add vertex and ensure it is properly decoded.
          g.addV(vertexLabel).property(propertyName, value).toList().then(result => {
            validateVertexResult(result, input[index], vertexLabel, propertyName);
            // Ensure the vertex is retrievable.
            g.V().hasLabel(vertexLabel).has(propertyName, value).toList().then(result => {
              validateVertexResult(result, input[index], vertexLabel, propertyName);
              timesNext();
            }).catch(timesNext);
          }).catch(timesNext);
        }, done);
      }));
    });

    describe('Traversal#toList()', function () {
      it('should return an Array of traversers', function () {
        return g.V().hasLabel('person').toList().then(people => {
          helper.assertInstanceOf(people, Array);
          people.forEach(function eachPerson(v) {
            helper.assertInstanceOf(v, dse.graph.Vertex);
            assert.ok(v.properties['name'][0].value);
          });
        });
      });

      it('should return an empty array of traversers when there is no match', function () {
        return g.V().hasLabel('notFound').toList().then(result => {
          helper.assertInstanceOf(result, Array);
          assert.strictEqual(result.length, 0);
        });
      });
    });

    describe('createExecutionProfile()', function() {
      const opts = helper.getOptions(helper.extend({}, {
        graphOptions: {name: 'name2'},
        profiles: [
          dseGraph.createExecutionProfile('analytics', {graphOptions: {source: 'a'}})
        ]
      }));

      const client = new Client(opts);

      before(helper.createModernGraph('name2'));
      after(client.shutdown.bind(client));

      // Get vertex clusters of people.  We expect there to be 2 clusters:
      // 1) A cluster with only Peter, he doesn't have a 'knows' relation to anybody.
      // 2) A cluster of vertices around Marko, he knows Vadas and Josh.
      // The peerPressure step requires a OLAP GraphComputer, thus this should fail without the
      // analytics traversal source.
      function executeAnalyticsQueries(traversal, expectPass) {
        const pass = expectPass === undefined ? true : expectPass;
        return (function () {
          const traversalP = traversal.V().hasLabel("person")
            .peerPressure().by("cluster")
            .group().by("cluster").by("name")
            .toList();

          if (pass) {
            return traversalP.then(list => {
              const result = getFirst(list);
              const clusters = Object.keys(result).map(function(k) {
                return result[k].sort();
              }).sort(function(a, b) {
                return a.length - b.length;
              });
              assert.deepEqual(clusters, [ ['peter'], ['josh', 'marko', 'vadas']]);
            });
          }
          let executionError;
          return traversalP.catch(err => executionError = err).then(() => {
            assert.ok(executionError, 'It should have failed');
          });
        });
      }

      const a0 = createTraversal(client, {executionProfile: 'analytics'});
      const a1 = createTraversal(client, {graphSource: 'a'});
      const g = createTraversal(client);

      // should succeed since using 'a' source.
      it('should make an OLAP query using \'a\' traversal source', executeAnalyticsQueries(a0));
      // should succeed since using profile which specifies 'a' source.
      it('should make an OLAP query using profile with \'a\' traversal source', executeAnalyticsQueries(a1));
      // should fail since graph computer required.
      it('should make fail to make an OLAP query when using \'g\' traversal source', executeAnalyticsQueries(g, false));
    });
  });

  vdescribe('6.0', 'queryFromBatch()', function () {
    it('should produce a query string usable with DseClient#executeGraph()', wrapClientPromise(function (client) {
      const g = dseGraph.traversalSource(client);

      const batch = [
        g.addV('person').property('name', 'Matt').property('age', 12),
        g.addV('person').property('name', 'Olivia').property('age', 8),
        g.V().has('person', 'name', 'Matt').addE("knows").to(__.V().has('name', 'Olivia'))
      ];

      const query = dseGraph.queryFromBatch(batch);

      return client.executeGraph(query, null, { executionProfile: 'traversal' })
        .then(() => g.V().has('person', 'name', 'Matt').out('knows').values('name').next())
        .then(({ value }) => assert.strictEqual(value, 'Olivia'));
    }));
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
  const vertex = result[0];
  assert.strictEqual(vertex.label, vertexLabel);
  const prop = vertex.properties[propertyName][0];
  helper.assertInstanceOf(prop, dse.graph.VertexProperty);
  const propValue = prop.value;
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

function getFirst(list) {
  helper.assertInstanceOf(list, Array);
  assert.ok(list.length > 0, 'Excepted array of length greater than 0');
  const first = list[0];
  assert.notEqual(first, null);
  return first;
}