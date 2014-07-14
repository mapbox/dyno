var test = require('tap').test;
var fixtures = require('./fixtures');
var s = require('./setup');
var dyno = s.dyno;

test('setup', s.setup({createTableMs:200}));
test('create Table', function(t) {
    dyno.createTable(fixtures.test, function(err, resp){
        t.equal(err, null);
        t.deepEqual(resp, {});
        t.end();
    });
});

test('delete Table', function(t) {
    dyno.deleteTable(fixtures.test.TableName, function(err, resp){
        t.equal(err, null);
        t.deepEqual(resp, {});
        t.end();
    });
});

test('teardown', s.teardown);
