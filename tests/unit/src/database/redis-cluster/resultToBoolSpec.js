'use strict';

var assert = require('assert'),
    resultToBool = require('../../../../../src/database/redis-cluster/resultToBool');

describe('resultToBool', function () {
    it('should return true when the number 1 is given', function () {
        assert.strictEqual(resultToBool(1), true);
    });

    it('should return false when the string 1 is given', function () {
        assert.strictEqual(resultToBool('1'), false);
    });

    it('should return false when something else than 1 is given', function () {
        assert.strictEqual(resultToBool(42), false);
    });
});
