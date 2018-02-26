/**
 * Copyright DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';

const assert = require('assert');
const dse = require('dse-driver');
const dseGraph = require('../../index');
const Client = dse.Client;
const geometry = dse.geometry;
const Point = geometry.Point;
const Polygon = geometry.Polygon;
const LineString = geometry.LineString;
const helper = require('../helper');
const vit = helper.vit;
const vdescribe = helper.vdescribe;
const predicates = dseGraph.predicates;
const tinkerpop = require('gremlin-javascript');
const __ = tinkerpop.process.statics;
const P = tinkerpop.process.P;

vdescribe('5.0', 'DseGraph', function () {
  this.timeout(120000);
  before(helper.ccm.startAllTask(1, {workloads: ['graph', 'solr']}));
  after(helper.ccm.remove.bind(helper.ccm));
  describe('predicates', function () {
    const addressBookGraphName = 'address_book';
    const geo = predicates.geo;
    const search = predicates.search;
    before(helper.wrapClient(function (client, done) {
      helper.series([
        helper.toTask(client.executeGraph, client,
          'system.graph(name).ifNotExists().create()', { name: addressBookGraphName }, { graphName: null }),
        helper.toTask(client.executeGraph, client,
          helper.queries.graph.getAddressBookSchema(helper.getDseVersion()), null, { graphName: addressBookGraphName }),
        helper.toTask(client.executeGraph, client,
          helper.queries.graph.modernSchema, null, { graphName: addressBookGraphName }),
        function (next) {
          helper.eachSeries(helper.queries.graph.addressBookGraph, function (q, eachNext) {
            client.executeGraph(q, null, { graphName: addressBookGraphName }, eachNext);
          }, next);
        },
        helper.toTask(client.executeGraph, client,
          helper.queries.graph.modernGraph, null, { graphName: addressBookGraphName }),
        function (next) {
          helper.trace('Reindexing address_book.user_p');
          helper.ccm.exec(['node1', 'dsetool', 'reload_core', 'address_book.user_p', 'reindex=true'], next);
        }
      ], done);
    }));
    const client = newClientInstance(addressBookGraphName);
    const g = dseGraph.traversalSource(client, { graphName: addressBookGraphName });
    before(client.connect.bind(client));
    after(client.shutdown.bind(client));
    describe('using P', function () {
      it('should match using P.eq()',
        // Should match 'Paul Thomas Joe' since alias is 'mario'
        testTraversal(
          g.V().hasLabel("person").has("age", P.eq(29)).values('name'),
          ['marko']
        ));
      it('should match using P.neq()',
        // Should match 'Paul Thomas Joe' since alias is 'mario'
        testTraversal(
          g.V().hasLabel("person").has("age", P.neq(29)).values('name'),
          ['josh', 'vadas', 'peter']
        ));
      it('should match using P.gt()',
        // Should match 'josh' and 'peter' since ages are greater than 29
        testTraversal(
          g.V().hasLabel("person").has("age", P.gt(29)).values('name'),
          ['josh', 'peter']
        ));
      it('should match using P.gte()',
        // Should match 'marko', 'josh' and 'peter since ages are greater or equal than 29
        testTraversal(
          g.V().hasLabel("person").has("age", P.gte(29)).values('name'),
          ['marko', 'josh', 'peter']
        ));
      it('should match using P.lt()',
        // Should match 'vadas' since ages are less than 29
        testTraversal(
          g.V().hasLabel("person").has("age", P.lt(29)).values('name'),
          ['vadas']
        ));
      it('should match using P.lte()',
        // Should match 'marko', and 'vadas' since ages are less or equal than 29
        testTraversal(
          g.V().hasLabel("person").has("age", P.lte(29)).values('name'),
          ['marko', 'vadas']
        ));
      it('should match using P.between()',
        // Should match 'josh' since age is between 30 and 34
        testTraversal(
          g.V().hasLabel("person").has("age", P.between(30, 34)).values('name'),
          ['josh']
        ));
      it('should match using P.inside()',
        // Should match 'josh' since age is inside range 30 and 34
        testTraversal(
          g.V().hasLabel("person").has("age", P.inside(30, 34)).values('name'),
          ['josh']
        ));
      it('should match using P.outside()',
        // Should match 'josh' since age is outside range 30 and 34
        testTraversal(
          g.V().hasLabel("person").has("age", P.outside(29, 35)).values('name'),
          ['vadas']
        ));
      it('should match using P.within()',
        // Should match 'josh' and 'marko' since names are within list
        testTraversal(
          g.V().hasLabel("person").has("name", P.within('josh', 'marko', 'george')).values('name'),
          ['josh', 'marko']
        ));
      it('should match using P.without()',
        // Should match 'josh' and 'marko' since names are not on list
        testTraversal(
          g.V().hasLabel("person").has("name", P.without('vadas', 'peter', 'george')).values('name'),
          ['josh', 'marko']
        ));
    });
    describe('search', function () {
      describe('using text index', function () {
        it('should search by token()',
          // Should match 'Jill Alice' since description contains 'cold' ('Enjoys a very nice cold coca cola')
          // Should match 'George Bill Steve' since description contains 'cold' ('A cold dude')
          testTraversal(
            g.V().has('user', 'description', search.token('cold')).values('full_name'),
            [ 'Jill Alice', 'George Bill Steve' ]
          ));
        it('should search by tokenPrefix()',
          // Should match 'Paul Thomas Joe' since description contains a word starting with 'h' ('Lives by the hospital')
          // Should match 'James Paul Joe' since description contains a word starting with 'h' ('Likes to hang out')
          testTraversal(
            g.V().has('user', 'description', search.tokenPrefix('h')).values('full_name'),
            [ 'Paul Thomas Joe', 'James Paul Joe' ]
          ));
        it('should search by tokenRegex()',
          // Should match 'Paul Thomas Joe' since description contains 'hospital' ('Lives by the hospital')
          // Should match 'Jill Alice' since description contains 'nice' ('Enjoys a very nice cold coca cola')
          testTraversal(
            g.V().has('user', 'description', search.tokenRegex('(nice|hospital)')).values('full_name'),
            [ 'Paul Thomas Joe', 'Jill Alice' ]
          ));
        vit('5.1', 'should search by tokenFuzzy()',
          // Should match 'Paul Thomas Joe' since description contains 'Lives' ('Lives by the hospital') and tokenFuzzy is case insensitive.
          // Should match 'James Paul joe' since description contains 'Likes' ('Likes to hang out')
          testTraversal(
            g.V().has('user', 'description', search.tokenFuzzy('lives', 1)).values('full_name'),
            [ 'Paul Thomas Joe', 'James Paul Joe' ]
          ));
        vit('5.1', 'should search by phrase()',
          // Should match 'George Bill Steve' since 'A cold dude' is at distance of 0 for 'a cold'.
          // Should match 'Jill Alice' since 'Enjoys a very nice cold coca cola' is at distance of 2 for 'a cold'.
          testTraversal(
            g.V().has('user', 'description', search.phrase('a cold', 2)).values('full_name'),
            [ 'George Bill Steve', 'Jill Alice' ]
          ));
      });
      describe('using string index', function () {
        it('should search by prefix()',
          // Only one user with full_name starting with Paul.
          testTraversal(
            g.V().has('user', 'full_name', search.prefix('Paul')).values('full_name'),
            [ 'Paul Thomas Joe' ]
          ));
        it('should search by regex()',
          // Only two people with names containing pattern for Paul.
          testTraversal(
            g.V().has('user', 'full_name', search.regex('.*Paul.*')).values('full_name'),
            [ 'Paul Thomas Joe', 'James Paul Joe' ]
          ));
        vit('5.1', 'should search by fuzzy()',
          // Should match 'Paul Thomas Joe' since alias is 'mario'
          // Should match 'George Bill Steve' since alias is 'wario' watch matches 'mario' within a distance of 1.
          testTraversal(
            g.V().has('user', 'alias', search.fuzzy('mario', 1)).values('full_name'),
            [ 'Paul Thomas Joe', 'George Bill Steve' ]
          ));
        vit('5.1', 'should search by fuzzy() #norecords',
          // Should match no records
          testTraversal(
            g.V().has('user', 'alias', search.fuzzy('marlo', 0)).values('full_name'),
            []
          ));
      });
    });
    describe('geo', function () {
      describe('inside', function () {
        describe('should search by distance', function () {
          it('with default units',
            // Should only be two people within 2 degrees of (-92, 44) (Rochester, Minneapolis)
            testTraversal(
              g.V().has('user', 'coordinates', geo.inside(new Point(-92, 44), 2)).values('full_name'),
              [ 'Paul Thomas Joe', 'George Bill Steve' ]
            ));
          it('with degrees',
            // Should only be two people within 2 degrees of (-92, 44) (Rochester, Minneapolis)
            testTraversal(
              g.V().has('user', 'coordinates', geo.inside(new Point(-92, 44), 2, geo.unit.degrees)).values('full_name'),
              [ 'Paul Thomas Joe', 'George Bill Steve' ]
            ));
          it('with miles',
            // Should only be two people within 190 miles of Madison, WI (-89.39, 43.06) (Rochester, Chicago)
            // Minneapolis is too far away (~200 miles).
            testTraversal(
              g.V().has('user', 'coordinates', geo.inside(new Point(-89.39, 43.06), 190, geo.unit.miles)).values('full_name'),
              [ 'Paul Thomas Joe', 'James Paul Joe' ]
            ));
          it('with kilometers',
            // Should only be two people within 400 KM of Des Moines, IA (-93.60, 41.60) (Rochester, Minneapolis)
            // Chicago is too far away (~500 KM)
            testTraversal(
              g.V().has('user', 'coordinates', geo.inside(new Point(-93.60, 41.60), 400, geo.unit.kilometers)).values('full_name'),
              [ 'Paul Thomas Joe', 'George Bill Steve' ]
            ));
          it('with kilometers#all places',
            // Should only be two people within 400 KM of Des Moines, IA (-93.60, 41.60) (Rochester, Minneapolis)
            // Chicago is too far away (~500 KM)
            testTraversal(
              g.V().has('user', 'coordinates', geo.inside(new Point(-93.60, 41.60), 40075, geo.unit.kilometers)).values('full_name'),
              [ 'George Bill Steve', 'James Paul Joe', 'Jill Alice', 'Paul Thomas Joe' ]
            ));
          it('with meters',
            // Should only be on person within 350,000 M of Des Moines, IA (-93.60, 41.60) (Rochester)
            testTraversal(
              g.V().has('user', 'coordinates', geo.inside(new Point(-93.60, 41.60), 350000, geo.unit.meters)).values('full_name'),
              [ 'Paul Thomas Joe' ]
            ));
        });
        it('should search by polygon area',
          // 10 degrees within La Crosse, WI should include Chicago, Rochester and Minneapolis.  This is needed to filter
          // down the traversal set as using the search index Geo.inside(polygon) is not supported for search indices.
          // Filter further by an area that only Chicago and Rochester fit in. (Minneapolis is too far west).
          testTraversal(
            g.V().has('user', 'coordinates', geo.inside(new Point(-91.2, 43.8), 10))
              .local(__.has('coordinates', geo.inside(new Polygon([new Point(-82, 40), new Point(-92.5, 45), new Point(-95, 38), new Point(-82, 40)]))))
              .values('full_name'),
            [ 'Paul Thomas Joe', 'James Paul Joe' ]
          ));
        it('should throw TypeError when search inside LineString', function() {
          // Search is only possible for points with distance or polygon.
          assert.throws(() => {
            g.V().has('user', 'coordinates', geo.inside(new LineString(new Point(10.99, 20.02), new Point(14, 26)))).values('full_name');
            }, TypeError)
        });
      });
    });
  });
});

function testTraversal(t, expected) {
  return (function testTraversalFn(done) {
    t.toList()
      .then(returnValue => {
        setImmediate(() => {
          if (Array.isArray(expected)) {
            const sExpected = expected.slice().sort();
            const sReturn = returnValue.slice().sort();
            assert.deepEqual(sReturn, sExpected);
          }
          else {
            assert.strictEqual(returnValue, expected);
          }
          done();
        });
      })
      .catch(done);
  });
}

/**
 * @param {String} graphName
 * @return {Client}
 */
function newClientInstance(graphName) {
  return new Client(helper.getOptions(helper.extend({}, { graphOptions : { name: graphName } })));
}