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
const api = require('../../index');

describe('API', function () {
  it('should expose traversalSource() method', function () {
    assert.strictEqual(typeof api.traversalSource, 'function');
    assert.strictEqual(api.traversalSource.length, 2);
  });
  it('should expose queryFromTraversal() method', function () {
    assert.strictEqual(typeof api.queryFromTraversal, 'function');
  });
  it('should expose createExecutionProfile() method', function () {
    assert.strictEqual(typeof api.createExecutionProfile, 'function');
    assert.strictEqual(api.createExecutionProfile.name, 'createExecutionProfile');
  });
  it('should expose version', function () {
    assert.ok(api.version);
  });
  it('should expose predicates', function () {
    assert.ok(api.predicates);
    assert.ok(api.predicates.geo);
    assert.strictEqual(typeof api.predicates.geo.GeoP, 'function');
    assert.strictEqual(typeof api.predicates.geo.inside, 'function');
    assert.strictEqual(typeof api.predicates.geo.unit, 'object');
    assert.ok(api.predicates.search);
    assert.strictEqual(typeof api.predicates.search.token, 'function');
    assert.strictEqual(typeof api.predicates.search.tokenFuzzy, 'function');
    assert.strictEqual(typeof api.predicates.search.regex, 'function');
    assert.strictEqual(typeof api.predicates.search.tokenRegex, 'function');
  });
});