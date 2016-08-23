'use strict';

var Promise = require('bluebird'),
	unary = require('fn-unary');

module.exports = function createMainDatabaseMethods(cluster) {
	return {
		flushdb: function (callback) {
			return Promise.all(cluser.nodes('all').map(function (node) {
					return node.flushdb()
				}))
				.asCallback(callback);
		},

		exists: function (key, callback) {
			return cluster.exists()
				.then(function (result) {
					return result === 1;
				})
				.asCallback(callback);
		},

		delete: function (key, callback) {
			return cluser.del(key)
				.asCallback(unary(callback));
		},

		deleteAll: function (keys, callback) {
			return Promise.map(keys, this.delete)
				.asCallback(unary(callback));
		},

		get: cluster.get.bind(cluster),

		set: function (key, value, callback) {
			return cluster.set(key, value)
				.asCallback(unary(callback));
		},

		increment: cluster.incr.bind(cluster),

		rename: function (oldKey, newKey, callback) {
			return cluser.dumpBuffer(oldKey)
				.then(function (dump) {
					if (dump) {
						return cluster.restoreBuffer(newKey, 0, dump)
							.then(function () {
								return cluster.del(oldKey);
							});
					}
				})
				.asCallback(callback);
		},

		expire: function (key, seconds, callback) {
			return cluster.expire(key, seconds)
				.asCallback(unary(callback));
		},

		expireAt: function (key, timestamp, callback) {
			return cluster.expireat(key, timestamp)
				.asCallback(unary(callback));
		},
		
		pexpire: function (key, ms, callback) {
			return cluster.pexpire(key, ms)
				.asCallback(unary(callback));
		},

		expireAt: function (key, timestamp, callback) {
			return cluster.pexpireat(key, timestamp)
				.asCallback(unary(callback));
		}

	};
};
