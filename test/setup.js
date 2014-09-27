var test = require('tap').test;
var fixtures = require('./fixtures');
var Dynalite = require('dynalite');
var Dyno = require('../');
var _ = require('underscore');
var dynalite;

var setup;

module.exports = function(live) {
    if (setup) return setup;

    setup = {};

    var tableName = setup.tableName = 'dyno-test-' + Math.ceil(1000 * Math.random());

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

    setup.test = function(name, opts, callback) {
        if (typeof opts === 'function') {
            callback = opts;
            opts = {};
        }

        opts.timeout = live ? 120000 : 30000;

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
        dyno.createTable(table, function(err, resp){
            t.ifError(err, 'created table');
            t.end();
        });
    };

    setup.teardown = function(t) {
        if (live) return dyno.deleteTable(table, function(err) {
            t.ifError(err, 'deleted live DynamoDB table');
            t.end();
        });

        dynalite.close();
        t.end();
    };

    return setup;
};
