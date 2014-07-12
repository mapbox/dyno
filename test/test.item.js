var test = require('tap').test;
var s = require('./setup');
var dyno = s.dyno;


test('setup', s.setup());
test('setup table', s.setupTable);
test('get', function(t) {
    var item = {id: 'yo', range: 5};

    dyno.getItem(item, function (err, resp) {
        t.equal(err, null);
        t.end();
    });
});
test('teardown', s.teardown);

test('setup', s.setup());
test('setup table', s.setupTable);
test('put', function(t) {
    var item = {id: 'yo', range: 5};

    dyno.putItem(item, itemResp);
    function itemResp(err, resp) {
        t.equal(err, null);
        t.deepEqual(resp, {});
        dyno.getItem(item, getItem);
    }
    function getItem(err, resp) {
        t.equal(err, null);
        t.deepEqual(resp, {Item:{id: 'yo', range: 5}});
        t.end();
    }
});
test('teardown', s.teardown);
