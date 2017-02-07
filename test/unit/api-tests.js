/**
 * Copyright (C) 2016 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';

var assert = require('assert');
var api = require('../../index');

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
  it('should expose the glv', function () {
    assert.strictEqual(typeof api.tinkerpop, 'object');
    assert.ok(Object.keys(api.tinkerpop).length > 0);
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