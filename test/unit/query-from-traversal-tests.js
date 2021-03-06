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

const assert = require('assert');
const dseGraph = require('../../index');
const { graphProtocol } = require('../../lib/dse-remote-connection');
const predicates = dseGraph.predicates;
const geo = predicates.geo;
const search = predicates.search;
const { types, geometry } = require('cassandra-driver');
const Point = geometry.Point;
const Polygon = geometry.Polygon;
const tinkerpop = require('gremlin');
const __ = tinkerpop.process.statics;

describe('dseGraph', function () {
  describe('queryFromTraversal()', function () {
    it('should return the expected graphSON2 string', function () {
      const order = tinkerpop.process.order;
      const g = new tinkerpop.structure.Graph().traversal();
      [
        [ g.V(), '{"@type":"g:Bytecode","@value":{"step":[["V"]]}}' ],
        [ g.addV('orders').property('uid', types.Uuid.fromString('9907570a-3ac5-4ec2-8894-9530e0659d83')),
          '{"@type":"g:Bytecode","@value":{"step":[["addV","orders"],["property","uid",{"@type":"g:UUID","@value":' +
          '"9907570a-3ac5-4ec2-8894-9530e0659d83"}]]}}'],
        [ g.addV('person').property('blob', Buffer.from('010103', 'hex')),
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
        '{"@type":"g:Bytecode","@value":{"step":[["V"],["has","user","coordinates",{"@value":{"predicate":"inside","predicateType":"Geo","value":{"@type":"dse:Distance","@value":"DISTANCE((-91.2 43.8) 10)"}},"@type":"dse:P"}],["local",{"@type":"g:Bytecode","@value":{"step":[["has","coordinates",{"@value":{"predicate":"insideCartesian","predicateType":"Geo","value":{"@type":"dse:Polygon","@value":"POLYGON ((-82 40, -92.5 45, -95 38, -82 40))"}},"@type":"dse:P"}]]}}],["values","full_name"]]}}']
      ].forEach(function (item) {
        assert.strictEqual(dseGraph.queryFromTraversal(item[0]), item[1]);
      });
    });

    it('should return the expected graphSON3 string', function () {
      const protocol = graphProtocol.graphson3;

      const g = new tinkerpop.structure.Graph().traversal();
      [
        [ g.V(), '{"@type":"g:Bytecode","@value":{"step":[["V"]]}}' ],
        [ [ g.V() ], '{"@type":"g:List","@value":[{"@type":"g:Bytecode","@value":{"step":[["V"]]}}]}' ],
        [ g.addV('orders').property('uid', types.Uuid.fromString('9907570a-3ac5-4ec2-8894-9530e0659d83')),
          '{"@type":"g:Bytecode","@value":{"step":[["addV","orders"],["property","uid",{"@type":"g:UUID","@value":' +
          '"9907570a-3ac5-4ec2-8894-9530e0659d83"}]]}}']
      ].forEach(function (item) {
        assert.strictEqual(dseGraph.queryFromTraversal(item[0], protocol), item[1]);
      });
    });

    it('should throw a TypeError when protocol is not supported', () => {
      const g = new tinkerpop.structure.Graph().traversal();
      assert.throws(() => dseGraph.queryFromTraversal(g.V(), 'abc'), /Protocol '.*' not supported/);
    });
  });
});