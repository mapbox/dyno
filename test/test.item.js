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
    var item = {delete:{str: 'a', num: 12}};
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


test('setup', s.setup());
test('setup table', s.setupTable);
test('setup items', function(t) {
    var item = {id: 'yo', range: 5};

    dyno.putItem(item, itemResp);
    function itemResp(err, resp) {
        t.end();
    }
});

test('query - EQ', function(t) {

    dyno.query({id:{'EQ':'yo'}, range:{'EQ':5}}, itemResp);
    function itemResp(err, resp) {
        t.equal(err, null);
        t.deepEqual(resp, {count : 1, items : [{id : 'yo', range : 5 }]})
        t.end();
    }

});

test('query - BETWEEN', function(t) {

    dyno.query({id:{'EQ':'yo'}, range:{'BETWEEN':[4,6]}}, itemResp);
    function itemResp(err, resp) {
        t.equal(err, null);
        t.deepEqual(resp, {count : 1, items : [{id : 'yo', range : 5 }]})
        t.end();
    }

});

test('query - BETWEEN - list attributes', function(t) {

    dyno.query({id:{'EQ':'yo'}, range:{'BETWEEN':[4,6]}}, {attributes:['range']}, itemResp);
    function itemResp(err, resp) {
        t.equal(err, null);
        t.deepEqual(resp, {count : 1, items : [{range : 5 }]})
        t.end();
    }

});

test('teardown', s.teardown);
