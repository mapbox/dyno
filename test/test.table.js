var fixtures = require('./fixtures');
var _ = require('underscore');
var s = require('./setup')();
var test = s.test;
var dyno = s.dyno;

var table = _(fixtures.test).extend({TableName: s.tableName});

test('setup', s.setup({createTableMs:200}));
test('create Table', function(t) {
    dyno.createTable(table, function(err, resp){
        t.equal(err, null);
        t.deepEqual(resp, {});
        t.end();
    });
});

test('list Table', function(t) {
    dyno.listTables(function(err, res) {
        t.equal(err, null);
        res.TableNames = res.TableNames.filter(function(name) {
            return name.indexOf('dyno-test-') === 0;
        });
        t.deepEqual(res, { TableNames: [ s.tableName ] });
        t.end();
    });
});

test('delete Table', function(t) {
    dyno.deleteTable(s.tableName, function(err, resp){
        t.equal(err, null);
        t.deepEqual(resp, {});
        t.end();
    });
});

test('teardown', s.teardown);
