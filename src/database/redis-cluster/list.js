'use strict';

var unary = require('fn-unary');

module.exports = function createListDatabaseMehtods(cluster) {
	return {
		listPrepend: function (key, value, callback) {
			return cluster.lpush(key, value)
				.asCallback(unary(callback);
		},

		listAppend: function (key, value, callback) {
			return cluster.rpush(key, value)
				.asCallback(unary(callback);
		},

		listRemoveLast: cluster.rpop.bind(cluster),

		listRemoveAll: function (key, value, callback) {
			return cluster.lrem(key, 0, value)
				.asCallback(unary(callback));
		},

		listTrim: function (key, start, stop, callback) {
			return cluster.ltrim(key, start, stop)
				.asCallback(unary(callback));
		},

		getListRange: cluster.lrange.bind(cluster);
	};
};
