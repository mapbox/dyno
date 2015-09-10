var fixtures = require('./fixtures');
var tape = require('tape');
var crypto = require('crypto');
var queue = require('queue-async');
var _ = require('underscore');
var Dyno = require('..');
var Dynalite = require('dynalite');
var dynalite = Dynalite({
    createTableMs: 0,
    updateTableMs: 0,
    deleteTableMs: 0
});

function test(label, callback) {
    var name = crypto.randomBytes(4).toString('hex');
    var table = _({ TableName: name }).extend(fixtures.gsi);
    var dyno = Dyno({
        table: name,
        region: 'fake',
        endpoint: 'http://localhost:4567'
    });

    tape(label, function(t) {
        var end = t.end.bind(t);
        t.end = function() {
            queue(1)
                .defer(dyno.deleteTable, table)
                .defer(dynalite.close)
                .awaitAll(end);
        };

        queue(1)
            .defer(dynalite.listen.bind(dynalite), 4567)
            .defer(dyno.createTable, table)
            .awaitAll(function(err) {
                if (err) throw err;
                callback.call({ dyno: dyno }, t);
            });
    });
}

test('[queryBatchGet]', function(assert) {
    var data = fixtures.randomItems(300).map(function(item, i) {
        item.group = i < 150 ? 'a' : 'b';
        delete item.data;
        return item;
    });

    var dyno = this.dyno;

    dyno.putItems(data, function(err) {
        if (err) throw err;

        var query = { group: { EQ: 'a' } };
        dyno.queryBatchGet('gsi', 'id', query, function(err, items) {
            assert.ifError(err, 'success');
            var sorted = _(items).sortBy(function(item) {
                return Number(item.id.split(':')[1]);
            });
            assert.deepEqual(sorted, data.slice(0, 150), 'returned appropriate items');
            assert.end();
        });
    });
});
