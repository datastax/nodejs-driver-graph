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
  });
  it('should expose the glv', function () {
    assert.strictEqual(typeof api.tinkerpop, 'object');
    assert.ok(Object.keys(api.tinkerpop).length > 0);
  });
  it('should expose version', function () {
    assert.ok(api.version);
  });
});