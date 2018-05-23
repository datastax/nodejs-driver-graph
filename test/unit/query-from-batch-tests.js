/**
 * Copyright DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';

const assert = require('assert');
const tinkerpop = require('gremlin');
const dseGraph = require('../../index');

describe('dseGraph', function () {

  describe('queryFromBatch()', function () {

    it('should return the expected graphSON2 string', function () {
      const g = new tinkerpop.structure.Graph().traversal();

      const batch = [
        g.addV('person').property('name', 'Matt').property('age', 12),
        g.addV('person').property('name', 'Olivia').property('age', 8)
      ];

      const query = dseGraph.queryFromTraversal(batch);
      assert.strictEqual(query, '[' +
        '{"@type":"g:Bytecode","@value":{"step":[["addV","person"],["property","name","Matt"],["property","age",12]]}},' +
        '{"@type":"g:Bytecode","@value":{"step":[["addV","person"],["property","name","Olivia"],["property","age",8]]}}' +
        ']');
    });

    it('should throw when the provided parameter is not an Array', function () {
      [
        'abc',
        true,
        {},
        1
      ].forEach(x => {
        assert.throws(() => dseGraph.queryFromBatch(x), /Batch parameter must be an Array of traversals/);
      });
    });
  });
});