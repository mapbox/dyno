var AWS = require('aws-sdk');
var s = require('./setup')(true);
var test = s.test;
var dynamoRequest = require('../lib/dynamoRequest');
var fixtures = require('./fixtures');
var _ = require('underscore');
var queue = require('queue-async');

var dyno = s.dyno;

test('delete fast table', s.deleteTable);

test('setup slow table', function(t) {
    var table = _({}).extend(fixtures.test, {TableName: s.tableName});
    dyno.createTable(table, function(err) {
        t.ifError(err, 'created slow table');
        t.end();
    });
});

test('slow enough', function(t) {
    var item = { id: '1', range: 1 };
    var attempts = 0;
    var max = 10;

    function put() {
        setTimeout(function() {
            dyno.putItem(item, { capacity: 'TOTAL' }, function(err, res, capacity) {
                t.ifError(err, 'put item ' + attempts);
                attempts++;
                if (attempts < max) put();
                else t.end();
            });
        }, 1000);
    }

    put();
});

test('too fast', function(t) {
    var items = fixtures.randomItems(10, 63 * 1024);
    var q = queue(1);

    items.forEach(function(item) {
        q.defer(dyno.putItem, item, { throughputAttempts: 1 });
    });

    q.awaitAll(function(err, results) {
        t.ok(err, 'expected err');
        if (!err) return t.end();

        t.equal(err.code, 'ProvisionedThroughputExceededException', 'expected throughput error');
        t.end();
    });
});

test('should throttle', function(t) {
    var items = fixtures.randomItems(10, 63 * 1024);
    var q = queue();

    items.forEach(function(item) {
        q.defer(dyno.putItem, item, { throughputAttempts: 2 });
    });

    q.awaitAll(function(err, results) {
        // Couldn't throttle enough. Check that at least throttling was tried
        if (err) t.equal(err.retryDelay, 100, 'tried to throttle requests');

        // Throttled enough
        else t.equal(results.length, items.length, 'all requests completed');

        t.end();
    });
});

test('can overshoot aws default retry limits', function(t) {
    var items = fixtures.randomItems(10, 63 * 1024);
    var q = queue();

    items.forEach(function(item) {
        q.defer(dyno.putItem, item, { throughputAttempts: 11 });
    });

    q.awaitAll(function(err, results) {
        // Couldn't throttle enough. Check that at least throttling was tried
        if (err) t.equal(err.retryDelay, 51200, 'tried to throttle requests');

        // Throttled enough
        else t.equal(results.length, items.length, 'all requests completed');

        t.end();
    });
});

test('delete slow table', s.deleteTable);
