var s = require('./setup');
var dyno = s.dyno;
var test = require('tap').test;

test('setup', s.setup());
test('setup table', s.setupTable);
test('update', function(t) {
    var item = {put:{str: 'a', num: 12}, add:{count:1}};
    var key = {id:'yo', range:5}

    dyno.updateItem(key, item, itemResp);
    function itemResp(err, resp) {
        t.equal(err, null);
        t.deepEqual(resp, {});
        dyno.getItem(key, getItem);
    }
    function getItem(err, resp) {
        t.equal(err, null);
        t.deepEqual(resp, {Item:{id: 'yo', range: 5, str: 'a', num:12, count:1}});
        t.end();
    }
});
test('update - delete', function(t) {
    var item = {delete:['str','num']};
    var key = {id:'yo', range:5}

    dyno.updateItem(key, item, itemResp);
    function itemResp(err, resp) {
        t.equal(err, null);
        t.deepEqual(resp, {});
        dyno.getItem(key, getItem);
    }
    function getItem(err, resp) {
        t.equal(err, null);
        t.deepEqual(resp, {Item:{id: 'yo', range: 5, count:1}});
        t.end();
    }
});
test('teardown', s.teardown);

test('setup', s.setup({createTableMs:200}));
test('setup table', s.setupTable);
test('update Item ', function(t) {

    var key = { id: 'yo', range: 5 };
    var actions = {put: { newkey: 'hi' }};
    var d = dyno.updateItem(key, actions, function(err, resp){
        t.notOk(err);
        dyno.getItem(key, function(err, data){
            t.notOk(err, 'no error');
            t.deepEqual(data, {
                "Item" : {
                    "id" : "yo",
                    "range" : 5,
                    "newkey" : "hi",
                }
            }, 'item was really updated');
            t.end();
        });
    });
});


test('update Item ', function(t) {

    var key = { id: 'yo', range: 5 };
    var actions = {put: { anothernewkey: 'hi' }, delete: ['newkey'], add: {counter: 1}};
    var d = dyno.updateItem(key, actions, function(err, resp){
        t.notOk(err);
        dyno.getItem(key, function(err, data){
            t.notOk(err, 'no error');
            t.deepEqual(data, {
                "Item" : {
                    "id" : "yo",
                    "range" : 5,
                    "anothernewkey" : "hi",
                    "counter" : 1
                }
            }, 'item was really updated');
            t.end();
        });
    });
});

test('update Item - with condition', function(t) {

    var key = { id: 'yo', range: 5 };

    var actions = {put: {anotherkey: 'hello'}, delete: ['otherrange'], add: {counter: 1}};
    var d = dyno.updateItem(key, actions, {
        expects: { "range": {"EQ" : [5] }}
    }, function(err, resp){
        t.notOk(err);
        t.end();
    });
});


test('update Item - with condition. fail conditions', function(t) {

    var key = { id: 'yo', range: 5 };

    var actions = {put: {anotherkey: 'hello'}};
    var d = dyno.updateItem(key, actions, {
    expected: { "anotherkey": { "EQ" : ['hi'] }}
    }, function(err, resp){
        t.ok(err, 'should return error');
        t.equal(err.message, 'The conditional request failed');
        t.equal(err.code, 'ConditionalCheckFailedException');
        t.end();
    });
});

test('teardown', s.teardown);
