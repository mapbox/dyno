var s = require('./setup')();
var test = s.test;
var es = require('event-stream');
var seedrandom = require('seedrandom');
var dyno = s.dyno;
var randomItems = require('./fixtures').randomItems;

test('setup', s.setup());
test('setup table', s.setupTable);
test('putItems', function(t) {
    var items = randomItems(1000);

    dyno.putItems(items, { capacity: 'TOTAL' }, itemResp);
    function itemResp(err, resp, metas) {
        t.equal(err, null);
        t.ok(metas, 'returned metadata object');
        t.ok(metas[0].capacity, 'returned capacity info');

        dyno.scan({pages:0, capacity: 'TOTAL'}, function(err, items, metas) {
            t.ifError(err, 'completed scan');
            if (err) return t.end();
            t.equal(items.length, 1000, 'there are the right number of items in dynamo');
            t.ok(metas, 'returned metadata object');
            t.ok(metas[0].capacity, 'returned capacity info');
            t.end();
        });
    }
});

test('getItems', function(t) {
    var itemIds = randomItems(200).map(function(item) {
        return {
            id: item.id,
            range: item.range
        };
    });

    dyno.getItems(itemIds, { capacity: 'TOTAL' }, function(err, items, metas) {
        t.ifError(err, 'got items');
        if (err) return t.end();

        t.ok(metas, 'returned metadata object');
        t.ok(metas[0].capacity, 'returned capacity info');

        t.equal(items.length, 200, 'got 200 items');
        t.end();
    });
});

test('deleteItems', function(t) {
    var itemIds = randomItems(1000).map(function(item) {
        return {
            id: item.id,
            range: item.range
        };
    });

    dyno.deleteItems(itemIds, { capacity: 'TOTAL' }, itemResp);
    function itemResp(err, resp, metas) {
        t.equal(err, null);
        t.ok(metas, 'returned metadata object');
        t.ok(metas[0].capacity, 'returned capacity info');

        dyno.scan({pages:0, capacity: 'TOTAL'}, function(err, items, metas) {
            t.equal(items.length, 0, 'there are the right number of items in dynamo');
            t.ok(metas, 'returned metadata object');
            t.ok(metas[0].capacity, 'returned capacity info');
            t.end();
        });
    }
});

test('teardown', s.teardown);
