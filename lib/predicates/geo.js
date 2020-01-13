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

const util = require('util');
const glv = require('gremlin');
const { geometry } = require('cassandra-driver');
const P = glv.process.P;

const degreesToRadians = Math.PI / 180;
const earthMeanRadiusKm = 6371.0087714;
const degreesToKm = degreesToRadians * earthMeanRadiusKm;
const kmToDegrees = 1 / degreesToKm;
const kmToMiles = 0.621371192;
const milesToKm = 1 / kmToMiles;

/**
 * Predicates for geolocation usage with DseGraph and Search indexes.
 * @module predicates/geo
 */

/**
 * Search any instance of a certain token within the text property targeted.
 * @param {Point|Polygon} centerOrShape The center of the area or the shape to look into.
 * @param {Number} [radius] The radius of the circle to look into.
 * @param {Number} [unit] The unit of the radius, as defined by the {@link unit} member. Defaults to
 * <code>degrees</code>.
 * @return {GeoP}
 */
function inside(centerOrShape, radius, unit) {
  if (centerOrShape instanceof geometry.Point) {
    validateUnit(unit);
    return new GeoP('inside', new Distance(centerOrShape, toDegrees(radius, unit)));
  }
  if (!(centerOrShape instanceof geometry.Polygon)) {
    throw new TypeError('geo.inside() only supports Polygons or Points with distance');
  }
  return new GeoP('insideCartesian', centerOrShape);
}

function toDegrees(distance, unit) {
  return distance * (unit || 1);
}

/**
 * The units of length definitions that the GEO search predicate can handle.
 * @type {Object}
 * @property {Number} miles Equal to 5,280 feet, or 1,760 yards, and standardised as exactly 1,609.344 metres.
 * @property {Number} kilometers Equal to one thousand meters.
 * @property {Number} meters The base unit of length in the International System of Units (SI).
 * @property {Number} degrees The degree of arc, defined so that a full rotation is 360 degrees.
 */
const unit = {
  miles: milesToKm * kmToDegrees,
  kilometers: kmToDegrees,
  meters: kmToDegrees / 1000,
  degrees: 1
};

function validateUnit(value) {
  if (!value) {
    return;
  }
  if (typeof value !== 'number') {
    throw new TypeError('Unit must be a number');
  }
  if (value !== unit.miles && value !== unit.kilometers && value !== unit.meters && value !== unit.degrees) {
    throw new TypeError('Unit value is not part of the unit member');
  }
}

/**
 * Represents a geometry predicate.
 * @param {String} operator
 * @param {Object} value
 * @extends {P}
 * @constructor
 */
function GeoP(operator, value) {
  this.operator = operator;
  this.value = value;
}

util.inherits(GeoP, P);

/**
 * Creates a new {@link Distance} instance.
 * @classdesc
 * A Distance is a circle in a two-dimensional XY plane represented by its
 * center point and radius.  It is used as a search criteria to determine
 * whether or not another geospatial object lies within a circular area.
 * @param {geometry.Point} center The center point.
 * @param {Number} radius The radius of the circle.
 * @ignore
 * @constructor
 */
function Distance(center, radius) {
  if(!(center instanceof geometry.Point)) {
    throw new TypeError('center must be an instanceof Point');
  }
  if(isNaN(radius)) {
    throw new TypeError('radius must be a number');
  }

  /**
   * Returns the center point.
   * @type {geometry.Point}
   */
  this.center = center;
  /**
   * Returns the radius of the circle.
   * @type {Number}
   */
  this.radius = radius;
}

/**
 * Returns DISTANCE((center.x, center.y), radius)).
 * @returns {String}
 */
Distance.prototype.toString = function () {
  return util.format('DISTANCE((%d %d) %d)', this.center.x, this.center.y, this.radius);
};


exports.GeoP = GeoP;
exports.inside = inside;
exports.unit = unit;
exports.Distance = Distance;