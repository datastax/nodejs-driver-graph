/**
 * Copyright (C) 2017 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';

var util = require('util');
var glv = require('../tinkerpop');
var P = glv.process.P;

/**
 * Search predicates module containing text and token matching predicates.
 * @module predicates/search
 */

/**
 * Search any instance of a certain token within the text property targeted.
 * @param value The value to look for.
 * @return {P}
 */
function token(value) {
  return new P('token', value);
}

/**
 * Search any instance of a certain token prefix withing the text property targeted.
 * @param value The value to look for.
 * @return {P}
 */
function tokenPrefix(value) {
  return new P('tokenPrefix', value);
}

/**
 * Search any instance of the provided regular expression for the targeted property.
 * @param value The value to look for.
 * @return {P}
 */
function tokenRegex(value) {
  return new P('tokenRegex', value);
}

/**
 * Search for a specific prefix at the beginning of the text property targeted.
 * @param value The value to look for.
 * @return {P}
 */
function prefix(value) {
  return new P('prefix', value);
}

/**
 * Search for this regular expression inside the text property targeted.
 * @param value The regular expression.
 * @return {P}
 */
function regex(value) {
  return new P('regex', value);
}

/**
 * Supports finding words which are a within a specific distance away (case insensitive).
 * <p>
 * Example: the search expression is <code>phrase("Hello world", 2)</code>
 * </p>
 * <ul>
 *   <li>the inserted value "Hello world" is found</li>
 *   <li>the inserted value "Hello wild world" is found</li>
 *   <li>the inserted value "Hello big wild world" is found</li>
 *   <li>the inserted value "Hello the big wild world" is not found</li>
 *   <li>the inserted value "Goodbye world" is not found.</li>
 * </ul>
 * @param {String} query the string to look for in the value
 * @param {Number} distance the number of terms allowed between two correct terms to find a value.
 * @return {TextDistanceP}
 */
function phrase(query, distance) {
  return new TextDistanceP('phrase', query, distance);
}

/**
 * Supports fuzzy searches based on the Levenshtein Distance, or Edit Distance algorithm
 * (case sensitive).
 * <p>Example: the search expression is <code>fuzzy("david", 1)</code></p>
 * <ul>
 *   <li>the inserted value "david" is found</li>
 *   <li>the inserted value "dawid" is found</li>
 *   <li>the inserted value "davids" is found</li>
 *   <li>the inserted value "dewid" is not found</li>
 * </ul>
 * @param {String} query the string to look for in the value
 * @param {Number} distance the number of "uncertainties" allowed for the Leveinshtein algorithm.
 * @return {TextDistanceP}
 */
function fuzzy(query, distance) {
  return new TextDistanceP('fuzzy', query, distance);
}

/**
 * Supports fuzzy searches based on the Levenshtein Distance, or Edit Distance algorithm
 * after having tokenized the data stored (case insensitive).
 * <p>Example: the search expression is </code>tokenFuzzy("david", 1)</code></p>
 * <ul>
 *   <li>the inserted value "david" is found</li>
 *   <li>the inserted value "dawid" is found</li>
 *   <li>the inserted value "hello-dawid" is found</li>
 *   <li>the inserted value "dewid" is not found</li>
 * </ul>
 * @param {String} query the string to look for in the value
 * @param {Number} distance the number of "uncertainties" allowed for the Leveinshtein algorithm.
 * @return {TextDistanceP}
 */
function tokenFuzzy(query, distance) {
  return new TextDistanceP('tokenFuzzy', query, distance);
}

/**
 * Represents a text search predicate with distance
 * @param operator
 * @param value
 * @param distance
 * @extends {P}
 * @constructor
 */
function TextDistanceP(operator, value, distance) {
  this.operator = operator;
  this.value = value;
  this.distance = distance;
}

util.inherits(TextDistanceP, P);

exports.fuzzy = fuzzy;
exports.tokenFuzzy = tokenFuzzy;
exports.phrase = phrase;
exports.prefix = prefix;
exports.regex = regex;
exports.TextDistanceP = TextDistanceP;
exports.token = token;
exports.tokenPrefix = tokenPrefix;
exports.tokenRegex = tokenRegex;