'use strict';

var unary = require('fn-unary'),
	resultToBool = require('./resultToBool');

module.exports = function createHashDatabaseMethods(cluster) {
	return {
		setAdd: function (key, value, callback) {
			return cluster.sadd(key, value)
				.asCallback(unary(callback));
		},

		setsAdd: function (keys, value, callback) {
			return Promise.map(keys, function (key) {
					return cluster.sadd(key, value);
				})
				.asCallback(unary(callback));
		},

		setRemove: function (key, value, callback) {
			return cluster.srem(key, value)
				.asCallback(unary(callback));
		},

		setsRemove: function (keys, value, callback) {
			return Promise.map(keys, function (key) {
					return cluster.srem(key, value);
				})
				.asCallback(unary(callback));
		},

		isSetMember: function (key, value, callback) {
			return cluster.sismember(key, value)
				.then(resultToBool)
				.asCallback(callback);
		},

		isSetMembers: function (key, values, callback) {
			var isSetMember = this.isSetMember;

			return Promise.map(values, function (value) {
					return isSetMember(key, value);
				})
				.asCallback(callback);
		},

		isMemberOfSets: function (sets, value, callback) {
			var isSetMember = this.isSetMember;

			return Promise.map(sets, function (set) {
					return isSetMember(set, value);
				})
				.asCallback(callback);
		},

		getSetMembers: cluster.smembers.bind(cluster),

		getSetsMembers: function (keys, callback) {
			return Promise.map(keys, function (key) {
					return cluster.smembers(key);
				})
				.asCallback(callback);
		},

		setCount: cluster.scard.bind(cluster),

		setsCount: function (keys, callback) {
			return Promise.map(keys, function (key) {
					return cluster.scard(key);
				})
				.asCallback(callback);
		},

		setRemoveRandom: cluster.spop.bind(cluster)
	};
};
