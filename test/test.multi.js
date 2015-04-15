var s = require('./setup')('multi');
var test = s.test;
var queue = require('queue-async');
var fixtures = require('./fixtures');
var _ = require('underscore');

test('setup', s.setup());
test('createTable', function(t) {
    var table = _(fixtures.test).extend({TableName: s.tableName});
    s.dyno.createTable(table, function(err) {
        t.ifError(err, 'no error');

        queue()
            .defer(s.readDyno.listTables.bind(s.readDyno))
            .defer(s.writeDyno.listTables.bind(s.writeDyno))
            .await(function(err, readTables, writeTables) {
                t.ifError(err, 'no error');

                readTables.TableNames = readTables.TableNames.filter(function(name) {
                    return name.indexOf('dyno-test-') === 0;
                });
                t.deepEqual(readTables, { TableNames: [s.tableName] });

                writeTables.TableNames = writeTables.TableNames.filter(function(name) {
                    return name.indexOf('dyno-test-') === 0;
                });
                t.deepEqual(writeTables, { TableNames: [s.tableName] });

                t.end();
            });
    });
});
test('deleteTable', function(t) {
    s.dyno.deleteTable(s.tableName, function(err) {
        t.ifError(err, 'no error');
        queue()
            .defer(s.readDyno.listTables.bind(s.readDyno))
            .defer(s.writeDyno.listTables.bind(s.writeDyno))
            .await(function(err, readTables, writeTables) {
                t.ifError(err, 'no error');
                t.deepEqual(readTables, { TableNames: [] });
                t.deepEqual(writeTables, { TableNames: [] });
                t.end();
            });
    });
});
test('teardown', s.teardown);

test('setup', s.setup());
test('setup table', s.setupTable);
test('getItem', function(t) {
    var items = fixtures.randomItems(2);
    s.readDyno.putItems(items, function(err) {
        if (err) throw err;
        s.dyno.getItem({ id: items[0].id, range: items[0].range }, function(err, found) {
            t.ifError(err, 'no error');
            t.deepEqual(found, items[0], 'expected item found');
            t.end();
        });
    });
});
test('teardown', s.teardown);

test('setup', s.setup());
test('setup table', s.setupTable);
test('putItem', function(t) {
    var item = fixtures.randomItems(1)[0];
    s.dyno.putItem(item, function(err) {
        t.ifError(err, 'no error');
        queue(1)
            .defer(s.readDyno.getItem, { id: item.id, range: item.range })
            .defer(s.writeDyno.getItem, { id: item.id, range: item.range })
            .await(function(err, readResult, writeResult) {
                t.ifError(err, 'no error');
                t.equal(readResult, undefined, 'no records in read table');
                t.deepEqual(writeResult, item, 'written to write table');
                t.end();
            });
    });
});
test('teardown', s.teardown);

test('setup', s.setup());
test('setup table', s.setupTable);
test('updateItem', function(t) {
    var item = fixtures.randomItems(1)[0];
    var key = { id: item.id, range: item.range };
    s.writeDyno.putItem(item, function(err) {
        if (err) throw err;
        s.dyno.updateItem(key, { put: { updated: 'ham' } }, assertions);
    });

    function assertions(err) {
        t.ifError(err, 'no error');
        queue(1)
            .defer(s.readDyno.getItem, key)
            .defer(s.writeDyno.getItem, key)
            .await(function(err, readResult, writeResult) {
                t.ifError(err, 'no error');
                t.equal(readResult, undefined, 'no records in read table');
                t.deepEqual(writeResult, _(item).extend({updated: 'ham'}), 'updated write table');
                t.end();
            });
    }
});
test('teardown', s.teardown);

test('setup', s.setup());
test('setup table', s.setupTable);
test('deleteItem', function(t) {
    var item = fixtures.randomItems(1)[0];
    var key = { id: item.id, range: item.range };
    s.writeDyno.putItem(item, function(err) {
        if (err) throw err;
        s.dyno.deleteItem(key, assertions);
    });

    function assertions(err) {
        t.ifError(err, 'no error');
        queue(1)
            .defer(s.readDyno.getItem, key)
            .defer(s.writeDyno.getItem, key)
            .await(function(err, readResult, writeResult) {
                t.ifError(err, 'no error');
                t.equal(readResult, undefined, 'no records in read table');
                t.deepEqual(writeResult, undefined, 'deleted from write table');
                t.end();
            });
    }
});
test('teardown', s.teardown);

test('setup', s.setup());
test('setup table', s.setupTable);
test('query', function(t) {
    var readItems = fixtures.randomItems(4);
    var writeItems = fixtures.randomItems(5);
    queue(1)
        .defer(s.readDyno.putItems, readItems)
        .defer(s.writeDyno.putItems, writeItems)
        .await(query);

    function query(err) {
        if (err) throw err;
        queue(1)
            .defer(s.dyno.query, {id: {EQ: readItems[0].id}, range: {EQ: readItems[0].range}})
            .defer(s.dyno.query, {id: {EQ: writeItems[4].id}, range: {EQ: writeItems[4].range}})
            .await(function(err, readResult, writeResult) {
                t.ifError(err, 'no error');
                t.deepEqual(readResult, [readItems[0]], 'queried read table');
                t.deepEqual(writeResult, [], 'did not query write table');
                t.end();
            });
    }
});
test('teardown', s.teardown);

test('setup', s.setup());
test('setup table', s.setupTable);
test('query: streaming interface', function(t) {
    var readItems = fixtures.randomItems(4);
    var writeItems = fixtures.randomItems(5);
    queue(1)
        .defer(s.readDyno.putItems, readItems)
        .defer(s.writeDyno.putItems, writeItems)
        .await(query);

    function query(err) {
        if (err) throw err;
        var readResult = [];
        var writeResult = [];

        queue(1)
            .defer(function(next) {
                s.dyno.query({id: {EQ: readItems[0].id}, range: {EQ: readItems[0].range}})
                    .on('data', function(item) {
                        readResult.push(item);
                    })
                    .on('end', next);
            })
            .defer(function(next) {
                s.dyno.query({id: {EQ: writeItems[4].id}, range: {EQ: writeItems[4].range}})
                    .on('data', function(item) {
                        writeResult.push(item);
                    })
                    .on('end', next);
            })
            .await(function(err) {
                t.ifError(err, 'no error');
                t.deepEqual(readResult, [readItems[0]], 'queried read table');
                t.deepEqual(writeResult, [], 'did not query write table');
                t.end();
            });
    }
});
test('teardown', s.teardown);

test('setup', s.setup());
test('setup table', s.setupTable);
test('scan', function(t) {
    var readItems = fixtures.randomItems(4);
    var writeItems = fixtures.randomItems(5);
    queue(1)
        .defer(s.readDyno.putItems, readItems)
        .defer(s.writeDyno.putItems, writeItems)
        .await(scan);

    function scan(err) {
        if (err) throw err;
        s.dyno.scan(function(err, result) {
            t.ifError(err, 'no error');
            t.equal(result.length, 4, 'scanned read table');
            t.end();
        });
    }
});
test('teardown', s.teardown);

test('setup', s.setup());
test('setup table', s.setupTable);
test('scan: streaming interface', function(t) {
    var readItems = fixtures.randomItems(4);
    var writeItems = fixtures.randomItems(5);
    var result = [];

    queue(1)
        .defer(s.readDyno.putItems, readItems)
        .defer(s.writeDyno.putItems, writeItems)
        .await(scan);

    function scan(err) {
        if (err) throw err;
        s.dyno.scan()
            .on('data', function(item) {
                result.push(item);
            })
            .on('end', function() {
                t.ifError(err, 'no error');
                t.equal(result.length, 4, 'scanned read table');
                t.end();
            });
    }
});
test('teardown', s.teardown);

test('setup', s.setup());
test('setup table', s.setupTable);
test('getItems', function(t) {
    var readItems = fixtures.randomItems(5);
    var keys = readItems.map(function(item) {
        return { id: item.id, range: item.range };
    });

    s.readDyno.putItems(readItems, function(err) {
        if (err) throw err;
        s.dyno.getItems(keys, function(err, results) {
            t.ifError(err, 'no error');
            t.equal(results.length, readItems.length, 'found all records');
            t.end();
        });
    });
});
test('teardown', s.teardown);

test('setup', s.setup());
test('setup table', s.setupTable);
test('putItems', function(t) {
    var items = fixtures.randomItems(5);
    s.dyno.putItems(items, function(err) {
        t.ifError(err, 'no error');
        queue()
            .defer(s.readDyno.scan)
            .defer(s.writeDyno.scan)
            .await(function(err, readResults, writeResults) {
                if (err) throw err;
                t.equal(readResults.length, 0, 'nothing written to read table');
                t.equal(writeResults.length, items.length, 'wrote to write table');
                t.end();
            });
    });
});
test('teardown', s.teardown);

test('setup', s.setup());
test('setup table', s.setupTable);
test('deleteItems', function(t) {
    var items = fixtures.randomItems(5);
    var keys = items.map(function(item) {
        return { id: item.id, range: item.range };
    });

    s.writeDyno.putItems(items, function(err) {
        if (err) throw err;
        s.dyno.deleteItems(keys, assertions);
    });

    function assertions(err) {
        t.ifError(err, 'no error');
        queue()
            .defer(s.readDyno.scan)
            .defer(s.writeDyno.scan)
            .await(function(err, readResults, writeResults) {
                if (err) throw err;
                t.equal(readResults.length, 0, 'nothing written to read table');
                t.equal(writeResults.length, 0, 'nothing left in write table');
                t.end();
            });
    }
});
test('teardown', s.teardown);

test('removes query-specific table', function(t) {
    function checkOpts() {
        var args = Array.prototype.slice.call(arguments);
        while (args.length > 2) args.shift();
        t.notOk(args.table, 'optional table was removed');
    }

    var read = {
        getItem: checkOpts,
        query: checkOpts,
        scan: checkOpts,
        getItems: checkOpts
    };

    var write = {
        putItem: checkOpts,
        updateItem: checkOpts,
        deleteItem: checkOpts,
        putItems: checkOpts,
        deleteItems: checkOpts
    };

    var multi = require('../lib/multi')(read, write);
    var cb = function() {};
    var opts = { table: 'nope' };
    multi.getItem({}, opts, cb);
    t.deepEqual(opts, { table: 'nope' }, 'did not clobber opts');
    multi.putItem({}, opts, cb);
    t.deepEqual(opts, { table: 'nope' }, 'did not clobber opts');
    multi.updateItem({}, {}, opts, cb);
    t.deepEqual(opts, { table: 'nope' }, 'did not clobber opts');
    multi.deleteItem({}, opts, cb);
    t.deepEqual(opts, { table: 'nope' }, 'did not clobber opts');
    multi.query({}, opts, cb);
    t.deepEqual(opts, { table: 'nope' }, 'did not clobber opts');
    multi.scan(opts, cb);
    t.deepEqual(opts, { table: 'nope' }, 'did not clobber opts');
    multi.getItems({}, opts, cb);
    t.deepEqual(opts, { table: 'nope' }, 'did not clobber opts');
    multi.putItems({}, opts, cb);
    t.deepEqual(opts, { table: 'nope' }, 'did not clobber opts');
    multi.deleteItems({}, opts, cb);
    t.deepEqual(opts, { table: 'nope' }, 'did not clobber opts');
    t.end();
});
