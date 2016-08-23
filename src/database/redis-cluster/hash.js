'use strict';

var unary = require('fn-unary'),
	_ = require('underscore'),
    resultToBool = require('./resultToBool');

function emptyObjectToNull(object) {
	if (_.isObject(object) && _.size(object) === 0) {
		return null;
	}

	return object;
}

module.exports = function createHashMethods(cluster) {

	return {
		setObject: function (key, data, callback) {
			return cluster.hmset(key, data)
				.asCallback(unary(callback));
		},

		setObjectField: function (key, field, value, callback) {
			return cluster.hset(key, field, value)
				.asCallback(unary(callback));
		},

		getObject: cluster.hgetall.bind(cluster),

		getObjects: function (keys, callback) {
			return Promise.map(keys, function (key) {
					return cluster.hgetall(key);
				})
				.map(emptyObjectToNull)
				.asCallback(callback);
		},

		getObjectField: cluster.hget.bind(cluster),

		getObjectFields: cluster.hmget.bind(cluster),

		getObjectsFields: function (keys, fields, callback) {
			var promise;

			if (!Array.isArray(fields) || !fields.length) {
				promise = Promise.resolve(keys.map(function() { return {}; }));
			} else {
				promise = Promise.map(keys, function (key) {
					return cluster.hmget(key, fields)
						.then(function (values) {
							return _.object(fields, values);
						});
				});
			}

			return promise.asCallback(callback);
		},

		getObjectKeys: cluster.hkeys.bind(cluster),

		getObjectValues: cluster.hvals.bind(cluster),

		isObjectField: function (key, field, callback) {
			return cluster.hexists(key, field)
				.then(resultToBool)
				.asCallback(callback);
		},

		isObjectFields: function (key, fields, callback) {
			var isObjectField = this.isObjectField;

			return Promise.map(fields, function (field) {
					return isObjectField(key, field);
				})
				.asCallback(callback);
		},

		deleteObjectField: function (key, field, callback) {
			return cluster.hdel(key, field)
				.asCallback(unary(callback);
		},

		deleteObjectFields: function (key, fields, callback) {
			var deleteObjectField = this.deleteObjectField;

			return Promise.map(fields, function (field) {
					return deleteObjectField(key, field)
				})
				.asCallback(unary(callback));
		},

		incrObjectField: function (key, field, callback) {
			return cluster.hincrby(key, field, 1)
				.asCallback(callback);
		},

		decrObjectField: function (key, field, callback) {
			return cluster.hincrby(key, field -1)
				.asCallback(callback);
		},

		incrObjectFieldBy: cluster.hincrby.bind(cluster)
	};
};
