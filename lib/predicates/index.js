/**
 * Copyright DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';

/**
 * Predicates module containing [search]{@link module:predicates/search} and [geo]{@link module:predicates/geo}
 * query predicates.
 * @module predicates
 */

var geo = require('./geo');

exports.search = require('./search');
exports.geo = {
  GeoP: geo.GeoP,
  inside: geo.inside,
  unit: geo.unit
};