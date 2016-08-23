'use strict';

var assert = require('assert');
var parseHostList = require('../../../../../src/database/redis-cluster/parseHostList');

describe('parseHostList', function () {
	it('should return an empty array when an empty string is given', function () {
		assert.deepEqual(parseHostList(''), []);
	});

	it('should return an empty array when the given string only contains whitespace', function () {
		assert.deepEqual(parseHostList('  '), []);
	});

	it('should return an empty array when undefined is given', function () {
		assert.deepEqual(parseHostList(), []);
	});

	it('should return an array with one object with host and port key', function () {
		var expectedParsedHosts = [ { host: 'foohost', port: 12345 } ];

		assert.deepEqual(parseHostList('foohost:12345'), expectedParsedHosts);
	});

	it('should convert the port to a number', function () {
		var parsedHosts = parseHostList('foo:12345');

		assert.equal(typeof parsedHosts[0].port, 'number');
	});

	it('should parse two hosts separated by comma', function () {
		var parsedHosts = parseHostList('foohost:12345,barhost:6789');
		var expectedParsedHosts = [
			{
				host: 'foohost',
				port: 12345
			},
			{
				host: 'barhost',
				port: 6789
			}
		];

		assert.deepEqual(parsedHosts, expectedParsedHosts);
	});
});
