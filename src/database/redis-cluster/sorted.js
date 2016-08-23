'use strict';

var Promise = require('bluebird'),
	_ = require('underscore'),
	unary = require('fn-unary'),
	utils = require('../../../public/src/utils.js');

function isOdd(value) {
	return value % 2 === 0;
}

function isKeyOdd(value, key) {
	return isOdd(key);
}

function flatListToPairs(list) {
	return _(list)
		.partition(isKeyOdd)
		.object()
		.pairs()
}

function sortedSetUnion(method, sets, start, stop, withScores, callback) {
	var uuid = utils.generateUUID();
	var tempUnionName = 'temp_' + uuid;
	var firstMaster = _.first(cluster.nodes('master'));
	var tempSets = sets.map(function (set) {
		return 'temp_' + set + '_' + uuid;
	});

	return Promise.map(sets, function (set, index) {
			var tempSet = tempSets[index];

			return cluster.dumpBuffer(set)
				.then(function (dump) {
					return firstMaster.restoreBuffer(tempSets, 0, dump, 'REPLACE');
				});
		})
		.then(function () {
			return firstMaster.zunionstore(tempUnionName, tempSets.length, tempSets)
		})
		.then(function () {
			return firstMaster[method](tempUnionName, start, stop, withScores);
		})
		.then(function (results) {
			return firstMaster.del(tempUnionName)
				.then(function () {
					return Promise.map(tempSets, function (tempSet) {
						return firstMaster.del(tempSet);
					});
				})
				.return(results || []);
		})
		.map(flatListToPairs)
		.map(function (result) {
			return { value: result[0], score: parseInt(result[1], 10) };
		});
		.asCallback(callback);

	var multi = redisClient.multi();
	multi.zunionstore([tempSetName, sets.length].concat(sets));
	multi[method](params);
	multi.del(tempSetName);
	multi.exec(function(err, results) {
		if (err) {
			return callback(err);
		}
		if (!withScores) {
			return callback(null, results ? results[1] : null);
		}
		results = results[1] || [];
		var objects = [];
		for(var i=0; i<results.length; i+=2) {
			objects.push({value: results[i], score: parseInt(results[i + 1], 10)});
		}
		callback(null, objects);
	});
}

function sortedSetRange(method, key, start, stop, withScores, callback) {
	if (Array.isArray(key)) {
		return sortedSetUnion(method, key, start, stop, withScores, callback);
	}

	var params = [key, start, stop];
	if (withScores) {
		params.push('WITHSCORES');
	}

	redisClient[method](params, function(err, data) {
		if (err) {
			return callback(err);
		}
		if (!withScores) {
			return callback(null, data);
		}
		var objects = [];
		for(var i=0; i<data.length; i+=2) {
			objects.push({value: data[i], score: parseInt(data[i + 1], 10)});
		}
		callback(null, objects);
	});
}

module.exports = function createSortedDatabaseMethods(cluster) {
	return {
		sortedSetAddMulti: function (key, scores, values, callback) {
			var promise;

			if (!scores.length || !values.length) {
				promise = Promise.resolve();
			} else if (scores.length !== values.length) {
				promise = Promise.reject(new Error('[[error:invalid-data]]'));
			} else {
				promise = cluster.zadd([ key ].concat(_.zip(scores, values)));
			}

			return promise.asCallback(unary(callback));
		},

		sortedSetAdd: function (key, score, value, callback) {
			if (Array.isArray(score) && Array.isArray(value)) {
				return this.sortedSetAddMulti(key, score, value, callback);
			}

			return cluster.zadd(key, score, value)
				.asCallback(unary(callback));
		},

		sortedSetsAdd: function (keys, score, value, callback) {
			return Promise.map(keys, function (key) {
					return cluster.zadd(key, score, value);
				})
				.asCallback(unary(callback));
		},

		sortedSetRemove: function (key, values, callback) {
			if (!Array.isArray(values) {
				values = [ values ];
			}

			return Promise.map(values, function (value) {
					return cluster.zrem(key, value);
				})
				.asCallback(unary(callback));
		},

		sortedSetsRemove: function (keys, value, callback) {
			return Promise.map(keys, function (key) {
					return cluster.zrem(key, value);
				})
				.asCallback(unary(callback));
		},

		sortedSetsRemoveRangeByScore: function (keys, min, max, callback) {
			return Promise.map(keys, function (key) {
					return cluster.zremrangebyscore(key, min, max);
				})
				.asCallback(unrary(callback));
		},

		getSortedSetRange: function () {
		}
	};
};

module.exports = function(redisClient, module) {

	module.getSortedSetRange = function(key, start, stop, callback) {
		sortedSetRange('zrange', key, start, stop, false, callback);
	};

	module.getSortedSetRevRange = function(key, start, stop, callback) {
		sortedSetRange('zrevrange', key, start, stop, false, callback);
	};

	module.getSortedSetRangeWithScores = function(key, start, stop, callback) {
		sortedSetRange('zrange', key, start, stop, true, callback);
	};

	module.getSortedSetRevRangeWithScores = function(key, start, stop, callback) {
		sortedSetRange('zrevrange', key, start, stop, true, callback);
	};

	
	module.getSortedSetRangeByScore = function(key, start, count, min, max, callback) {
		redisClient.zrangebyscore([key, min, max, 'LIMIT', start, count], callback);
	};

	module.getSortedSetRevRangeByScore = function(key, start, count, max, min, callback) {
		redisClient.zrevrangebyscore([key, max, min, 'LIMIT', start, count], callback);
	};

	module.getSortedSetRangeByScoreWithScores = function(key, start, count, min, max, callback) {
		sortedSetRangeByScoreWithScores('zrangebyscore', key, start, count, min, max, callback);
	};

	module.getSortedSetRevRangeByScoreWithScores = function(key, start, count, max, min, callback) {
		sortedSetRangeByScoreWithScores('zrevrangebyscore', key, start, count, max, min, callback);
	};

	function sortedSetRangeByScoreWithScores(method, key, start, count, min, max, callback) {
		redisClient[method]([key, min, max, 'WITHSCORES', 'LIMIT', start, count], function(err, data) {
			if (err) {
				return callback(err);
			}
			var objects = [];
			for(var i=0; i<data.length; i+=2) {
				objects.push({value: data[i], score: parseInt(data[i+1], 10)});
			}
			callback(null, objects);
		});
	}

	module.sortedSetCount = function(key, min, max, callback) {
		redisClient.zcount(key, min, max, callback);
	};

	module.sortedSetCard = function(key, callback) {
		redisClient.zcard(key, callback);
	};

	module.sortedSetsCard = function(keys, callback) {
		if (Array.isArray(keys) && !keys.length) {
			return callback(null, []);
		}
		var multi = redisClient.multi();
		for(var i=0; i<keys.length; ++i) {
			multi.zcard(keys[i]);
		}
		multi.exec(callback);
	};

	module.sortedSetRank = function(key, value, callback) {
		redisClient.zrank(key, value, callback);
	};

	module.sortedSetsRanks = function(keys, values, callback) {
		var multi = redisClient.multi();
		for(var i=0; i<values.length; ++i) {
			multi.zrank(keys[i], values[i]);
		}
		multi.exec(callback);
	};

	module.sortedSetRanks = function(key, values, callback) {
		var multi = redisClient.multi();
		for(var i=0; i<values.length; ++i) {
			multi.zrank(key, values[i]);
		}
		multi.exec(callback);
	};

	module.sortedSetRevRank = function(key, value, callback) {
		redisClient.zrevrank(key, value, callback);
	};

	module.sortedSetScore = function(key, value, callback) {
		redisClient.zscore(key, value, callback);
	};

	module.sortedSetsScore = function(keys, value, callback) {
		helpers.multiKeysValue(redisClient, 'zscore', keys, value, callback);
	};

	module.sortedSetScores = function(key, values, callback) {
		helpers.multiKeyValues(redisClient, 'zscore', key, values, callback);
	};

	module.isSortedSetMember = function(key, value, callback) {
		module.sortedSetScore(key, value, function(err, score) {
			callback(err, !!score);
		});
	};

	module.isSortedSetMembers = function(key, values, callback) {
		helpers.multiKeyValues(redisClient, 'zscore', key, values, function(err, results) {
			if (err) {
				return callback(err);
			}
			callback(null, results.map(Boolean));
		});
	};

	module.isMemberOfSortedSets = function(keys, value, callback) {
		helpers.multiKeysValue(redisClient, 'zscore', keys, value, function(err, results) {
			if (err) {
				return callback(err);
			}
			callback(null, results.map(Boolean));
		});
	};

	module.getSortedSetsMembers = function(keys, callback) {
		var multi = redisClient.multi();
		for (var i=0; i<keys.length; ++i) {
			multi.zrange(keys[i], 0, -1);
		}
		multi.exec(callback);
	};

	module.getSortedSetUnion = function(sets, start, stop, callback) {
		sortedSetUnion('zrange', sets, start, stop, false, callback);
	};

	module.getSortedSetRevUnion = function(sets, start, stop, callback) {
		sortedSetUnion('zrevrange', sets, start, stop, false, callback);
	};

	
	module.sortedSetIncrBy = function(key, increment, value, callback) {
		redisClient.zincrby(key, increment, value, callback);
	};

	module.getSortedSetRangeByLex = function(key, min, max, start, count, callback) {
		if (min !== '-') {
			min = '[' + min;
		}
		if (max !== '+') {
			max = '(' + max;
		}
		redisClient.zrangebylex([key, min, max, 'LIMIT', start, count], callback);
	};

	module.getSortedSetIntersect = function(params, callback) {
		params.method = 'zrange';
		getSortedSetRevIntersect(params, callback);
	};

	module.getSortedSetRevIntersect = function(params, callback) {
		params.method = 'zrevrange';
		getSortedSetRevIntersect(params, callback);
	};

	function getSortedSetRevIntersect (params, callback) {
		var sets = params.sets;
		var start = params.hasOwnProperty('start') ? params.start : 0;
		var stop = params.hasOwnProperty('stop') ? params.stop : -1;
		var weights = params.weights || [];

		var tempSetName = 'temp_' + Date.now();

		var interParams = [tempSetName, sets.length].concat(sets);
		if (weights.length) {
			interParams = interParams.concat(['WEIGHTS'].concat(weights));
		}

		if (params.aggregate) {
			interParams = interParams.concat(['AGGREGATE', params.aggregate]);
		}

		var rangeParams = [tempSetName, start, stop];
		if (params.withScores) {
			rangeParams.push('WITHSCORES');
		}

		var multi = redisClient.multi();
		multi.zinterstore(interParams);
		multi[params.method](rangeParams);
		multi.del(tempSetName);
		multi.exec(function(err, results) {
			if (err) {
				return callback(err);
			}

			if (!params.withScores) {
				return callback(null, results ? results[1] : null);
			}
			results = results[1] || [];
			var objects = [];
			for(var i=0; i<results.length; i+=2) {
				objects.push({value: results[i], score: parseFloat(results[i + 1])});
			}
			callback(null, objects);
		});
	}
};
