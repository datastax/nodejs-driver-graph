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
  it('should expose graphTraversalSource() method', function () {
    assert.strictEqual(typeof api.graphTraversalSource, 'function');
    assert.strictEqual(api.graphTraversalSource.length, 2);
  });
  it('should expose executeTraversal() method');
  it('should expose the glv', function () {
    assert.strictEqual(typeof api.tinkerpop, 'object');
    assert.ok(Object.keys(api.tinkerpop).length > 0);
  });
});