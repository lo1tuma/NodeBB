'use strict';

var winston = require('winston'),
	nconf = require('nconf'),
	semver = require('semver'),
	session = require('express-session'),
	Redis = require('ioredis'),
	_ = require('underscore'),
	createRedisSessionStoreMiddleware = require('connect-redis')(session),
	parseHostList = require('./redis-cluster/parseHostList'),
	createMainDatabaseMethods = require('./redis-cluster/main'),
	createHashDatabaseMethods = require('./redis-cluster/hash'),
	createSetDatabaseMethods = require('./redis-cluster/set'),
	createListDatabaseMethods = require('./redis-cluster/list');

function connectToRedisCluster() {
	var startupNodes = parseHostList(nconf.get('redis-cluster:startup-nodes'))
	   cluster = new Redis.Cluster(startupNodes);

	return cluster.connect()
		.then(function () {
			return cluster;
		});
}

function fetchDatabaseInfo(cluster, callback) {
	return cluster.info()
		.then(function (data) {
			var lines = data.toString().split("\r\n").sort();

			return lines.reduce(function (databaseInfo, line) {
				var parts = line.split(':');

				if (parts.length === 2) {
					return _.extend(databaseInfo, { parts[0]: parts[1] };
				}

				return databaseInfo;
			}, { redis: true });
		})
		.asCallback(callback);
}

function checkCompatibility(cluster, callback) {
	return fetchDatabaseInfo(cluster)
		.then(function (info) {
			if (semver.lt(info.redis_version, '3.0.0')) {
				throw new Error('The used redis version ' + info.redis_version + ' is too old. Cluster support requires at least redis v3.0.0.');
		})
		.asCallback(callback);
	});
};

function createRedisClusterAdapter() {
	return connectToRedisCluster()
		.then(function (cluster) {
			var twoWeeksInSeconds = 1209600,
				sessionStore = new connectRedis({
					client: cluster,
					ttl: twoWeeksInSeconds
				});

			return _.extend(
				{
					sessionStore: sessionStore
					info: fetchDatabaseInfo.bind(null, cluster),
					checkCompatibility: checkCompatibility.bind(null, cluster),
					close: function (callback) {
						return cluster.quit()
							.asCallback(callback);
					}
				},
				createMainDatabaseMethods(cluster),
				createHashDatabaseMethods(cluster),
				createSetDatabaseMethods(cluster),
				require('./redis-cluster/sorted')(cluster),
				createListDatabaseMethods(cluster)
			);
		});
}

module.exports = {
	init: function (callback) {
		var redisClusterAdapter = createRedisClusterAdapter();

		_.extend(module.exports, redisClusterAdapter);
	},

	questions: [
		{
			name: 'redis-cluster:start-nodes',
			description: 'List of host,port pairs that should be used as start nodes for connecting to the redis cluster. Example: example.com:6379,example-2.com:6379',
			'default': nconf.get('redis-cluster:start-nodes') || '127.0.0.1:6379'
		}
	]
};
