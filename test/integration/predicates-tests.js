/**
 * Copyright DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';

var util = require('util');
var assert = require('assert');
var dse = require('dse-driver');
var dseGraph = require('../../index');
var Client = dse.Client;
var geometry = dse.geometry;
var Point = geometry.Point;
var Polygon = geometry.Polygon;
var helper = require('../helper');
var vit = helper.vit;
var vdescribe = helper.vdescribe;
var predicates = dseGraph.predicates;
var tinkerpop = require('gremlin-javascript');
var __ = tinkerpop.process.statics;

vdescribe('5.0', 'DseGraph', function () {
  this.timeout(60000);
  before(helper.ccm.startAllTask(1, {workloads: ['graph', 'solr']}));
  after(helper.ccm.remove.bind(helper.ccm));
  describe('predicates', function () {
    var addressBookGraphName = 'address_book';
    var geo = predicates.geo;
    var search = predicates.search;
    before(helper.wrapClient(function (client, done) {
      helper.series([
        helper.toTask(client.executeGraph, client,
          'system.graph(name).ifNotExists().create()', { name: addressBookGraphName }, { graphName: null }),
        helper.toTask(client.executeGraph, client,
          helper.queries.graph.getAddressBookSchema(helper.getDseVersion()), null, { graphName: addressBookGraphName }),
        function (next) {
          helper.eachSeries(helper.queries.graph.addressBookGraph, function (q, eachNext) {
            client.executeGraph(q, null, { graphName: addressBookGraphName }, eachNext);
          }, next);
        },
        function (next) {
          helper.trace('Reindexing address_book.user_p');
          helper.ccm.exec(['node1', 'dsetool', 'reload_core', 'address_book.user_p', 'reindex=true'], next);
        }
      ], done);
    }));
    var client = newClientInstance(addressBookGraphName);
    var g = dseGraph.traversalSource(client, { graphName: addressBookGraphName });
    before(client.connect.bind(client));
    after(client.shutdown.bind(client));
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
      });
    });
  });
});

function testTraversal(t, expected) {
  return (function testTraversalFn(done) {
    t.toList(function (err, returnValue) {
      setImmediate(function () {
        assert.ifError(err);
        if (Array.isArray(expected)) {
          var sExpected = expected.slice().sort();
          var sReturn = returnValue.slice().sort();
          assert.deepEqual(sReturn, sExpected);
        }
        else {
          assert.strictEqual(returnValue, expected);
        }
        done();
      });
    });
  });
}

/**
 * @param {String} graphName
 * @return {Client}
 */
function newClientInstance(graphName) {
  return new Client(helper.getOptions(helper.extend({}, { graphOptions : { name: graphName } })));
}