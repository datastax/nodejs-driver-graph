/**
 * Copyright (C) 2016 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';

var assert = require('assert');
var dseGraph = require('../../index');
var predicates = dseGraph.predicates;
var geo = predicates.geo;
var search = predicates.search;
var dse = require('dse-driver');
var types = dse.types;
var geometry = dse.geometry;
var Point = geometry.Point;
var Polygon = geometry.Polygon;
var tinkerpop = dseGraph.tinkerpop;
var __ = tinkerpop.process.statics;

describe('dseGraph', function () {
  describe('queryFromTraversal()', function () {
    it('should return the expected graphSON2 string', function () {
      var order = dseGraph.tinkerpop.process.order;
      var g = new dseGraph.tinkerpop.structure.Graph().traversal();
      [
        [ g.V(), '{"@type":"g:Bytecode","@value":{"step":[["V"]]}}' ],
        [ g.addV('orders').property('uid', types.Uuid.fromString('9907570a-3ac5-4ec2-8894-9530e0659d83')),
          '{"@type":"g:Bytecode","@value":{"step":[["addV","orders"],["property","uid",{"@type":"g:UUID","@value":' +
          '"9907570a-3ac5-4ec2-8894-9530e0659d83"}]]}}'],
        [ g.addV('person').property('blob', new Buffer('010103', 'hex')),
          '{"@type":"g:Bytecode","@value":{"step":[["addV","person"],["property","blob",{"@type":"dse:Blob","@value":' +
          '"AQED"}]]}}'],
        [ g.V().hasLabel('person').has('age').order().by('age', order.decr),
          '{"@type":"g:Bytecode","@value":{"step":[["V"],["hasLabel","person"],["has","age"],["order"],["by","age",' +
          '{"@type":"g:Order","@value":"decr"}]]}}'],
        [ g.V().has('user', 'description', search.token('whatever') ),
          '{"@type":"g:Bytecode","@value":{"step":[["V"],["has","user","description",{"@value":{"predicate":"token","predicateType":"P","value":"whatever"},"@type":"dse:P"}]]}}'],
        [ g.V().has('user', 'description', geo.inside(new Point(-92, 44), 2)).values('full_name'),
          '{"@type":"g:Bytecode","@value":{"step":[["V"],["has","user","description",{"@value":{"predicate":"inside",' +
          '"predicateType":"Geo","value":{"@type":"dse:Distance","@value":"DISTANCE((-92 44) 2)"}},"@type":"dse:P"}],["values","full_name"]]}}'],
        [ g.V().has("user", "description", search.phrase("a cold", 2)).values("full_name"),
        '{"@type":"g:Bytecode","@value":{"step":[["V"],["has","user","description",{"@value":{"predicate":"phrase","value":{"query":"a cold","distance":2}},"@type":"g:P"}],["values","full_name"]]}}'],
        [ g.V().has('user', 'coordinates', geo.inside(new Point(-91.2, 43.8), 10))
            .local(__.has('coordinates', geo.inside(new Polygon([new Point(-82, 40), new Point(-92.5, 45), new Point(-95, 38), new Point(-82, 40)]))))
            .values('full_name'),
        '{"@type":"g:Bytecode","@value":{"step":[["V"],["has","user","coordinates",{"@value":{"predicate":"inside","predicateType":"Geo","value":{"@type":"dse:Distance","@value":"DISTANCE((-91.2 43.8) 10)"}},"@type":"dse:P"}],["local",{"@type":"g:Bytecode","@value":{"step":[["has","coordinates",{"@value":{"predicate":"inside","predicateType":"Geo","value":{"@type":"dse:Polygon","@value":"POLYGON ((-82 40, -92.5 45, -95 38, -82 40))"}},"@type":"dse:P"}]]}}],["values","full_name"]]}}']
      ].forEach(function (item) {
        assert.strictEqual(dseGraph.queryFromTraversal(item[0]), item[1]);
      });
    });
  });
});