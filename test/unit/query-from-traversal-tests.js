/**
 * Copyright (C) 2016 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';

var assert = require('assert');
var dseGraph = require('../../index');
var types = require('dse-driver').types;

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
          '{"@type":"g:Order","@value":"decr"}]]}}']
      ].forEach(function (item) {
        assert.strictEqual(dseGraph.queryFromTraversal(item[0]), item[1]);
      });
    });
  });
});