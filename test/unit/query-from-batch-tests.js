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