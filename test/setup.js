var test = require('tap').test;
var fixtures = require('./fixtures');
var Dynalite = require('dynalite');
var Dyno = require('../');
var _ = require('underscore');
var queue = require('queue-async');
var dynalite;

var setup;

module.exports = function(setting) {
    if (setup && setup.setting === setting) return setup;

    var live = setting === 'multi' ? false : !!setting;

    setup = { setting: setting };

    var tableName = setup.tableName = 'dyno-test-' + Math.ceil(1000 * Math.random());
    var tableExists = false;

    var table = live ?
        _({}).extend(fixtures.live, {TableName: tableName}) :
        _({}).extend(fixtures.test, {TableName: tableName});

    var config = live ?
        {
            table: tableName,
            region: 'us-east-1'
        } :
        {
            accessKeyId: 'fake',
            secretAccessKey: 'fake',
            region: 'us-east-1',
            table: tableName,
            endpoint: 'http://localhost:4567'
        };

    var dyno = setup.dyno = Dyno(config);

    if (setting === 'multi') {
        var read = {
            accessKeyId: 'fake',
            secretAccessKey: 'fake',
            region: 'us-east-1',
            table: tableName + '-read',
            endpoint: 'http://localhost:4567'
        };

        var write = {
            accessKeyId: 'fake',
            secretAccessKey: 'fake',
            region: 'us-east-1',
            table: tableName + '-write',
            endpoint: 'http://localhost:4567'
        };

        dyno = setup.dyno = Dyno.multi(read, write);

        setup.readDyno = Dyno(read);
        setup.writeDyno = Dyno(write);
    }

    setup.test = function(name, opts, callback) {
        if (typeof opts === 'function') {
            callback = opts;
            opts = {};
        }

        if (!opts.timeout) opts.timeout = live ? 120000 : 30000;

        test(name, opts, callback);
    };

    setup.setup = function(opts) {
        if (live) return function(t) {
            t.end();
        };

        if(!opts) opts = {};
        return function(t) {
            dynalite = Dynalite({
                createTableMs: opts.createTableMs || 0,
                updateTableMs: opts.updateTableMs || 0,
                deleteTableMs: opts.deleteTableMs || 0
            });
            dynalite.listen(4567, function() {
                t.end();
            });
        };
    };

    setup.setupTable = function(t) {
        if (live && tableExists) return t.end();

        if (setting === 'multi') {
            var tablename = table.TableName;
            queue(1)
                .defer(setup.readDyno.createTable, _({TableName: tablename + '-read'}).defaults(table))
                .defer(setup.writeDyno.createTable, _({TableName: tablename + '-write'}).defaults(table))
                .await(function(err) {
                    t.ifError(err, 'created tables');
                    t.end();
                });
        } else {
            dyno.createTable(table, function(err, resp) {
                t.ifError(err, 'created table');
                tableExists = true;
                t.end();
            });
        }
    };

    setup.teardown = function(t) {
        if (live) {
            // throw errors to force-quit on failure
            return dyno.scan({ pages: 0 }, function(err, items) {
                if (err) throw err;
                var keys = items.map(function(item) {
                    return {
                        id: item.id,
                        range: item.range
                    };
                });
                dyno.deleteItems(keys, function(err) {
                    t.ifError(err, 'truncated live table');
                    if (err) throw err;
                    t.end();
                });
            });
        }

        dynalite.close();
        t.end();
    };

    setup.deleteTable = function(t) {
        dyno.deleteTable(tableName, function(err) {
            t.ifError(err, 'deleted table');
            t.end();
        });
    };

    return setup;
};
