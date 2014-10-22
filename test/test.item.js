var s = require('./setup')();
var test = s.test;
var es = require('event-stream');
var dyno = s.dyno;
var _ = require('underscore');


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
        t.deepEqual(resp, item);
        dyno.getItem(item, getItem);
    }
    function getItem(err, resp) {
        t.equal(err, null);
        t.deepEqual(resp, item);
        t.end();
    }
});
test('teardown', s.teardown);

test('setup', s.setup());
test('setup table', s.setupTable);
test('conditional put', function(t) {
    var item = {id: 'yo', range: 5};

    dyno.putItem(item, itemResp);
    function itemResp(err, resp) {
        t.equal(err, null);
        t.deepEqual(resp, item);

        var options = {
            expected:{
                a:{'NULL': []}
            }
        };
        dyno.putItem(item, options, itemPut);
    }
    function itemPut(err, resp) {
        t.notOk(err, 'no error');
        t.end();
    }
});
test('teardown', s.teardown);

test('setup', s.setup());
test('setup table', s.setupTable);
test('conditional put', function(t) {
    var item = {id: 'yo', range: 5};

    dyno.putItem(item, itemResp);
    function itemResp(err, resp) {
        t.equal(err, null);
        t.deepEqual(resp, item);

        var options = {
            expected:{
                range:{'NE': [ { N: item.range.toString() } ]}
            }
        };
        dyno.putItem(item, options, expectFailure);
    }
    function expectFailure(err, resp) {
        t.ok(err, 'throws an error');
        t.equal(err.code, 'ConditionalCheckFailedException', 'expected error');
        t.end();
    }
});
test('teardown', s.teardown);


test('setup', s.setup());
test('setup table', s.setupTable);
test('setup items', function(t) {
    var item = {id: 'yo', range: 5, extra:'hi'};

    dyno.putItem(item, itemResp);
    function itemResp(err, resp) {
        t.end();
    }
});

test('query - EQ', function(t) {

    dyno.query({id:{'EQ':'yo'}, range:{'EQ':5}}, itemResp);
    function itemResp(err, resp) {
        t.equal(err, null);
        t.deepEqual(resp, [{id : 'yo', range : 5, extra: 'hi' }]);
        t.end();
    }

});

test('query - BETWEEN', function(t) {

    dyno.query({id:{'EQ':'yo'}, range:{'BETWEEN':[4,6]}}, itemResp);
    function itemResp(err, resp) {
        t.equal(err, null);
        t.deepEqual(resp, [{id : 'yo', range : 5, extra: 'hi' }]);
        t.end();
    }

});

test('query - BETWEEN - list attributes', function(t) {

    dyno.query({id:{'EQ':'yo'}, range:{'BETWEEN':[4,6]}}, {attributes:['range']}, itemResp);
    function itemResp(err, resp) {
        t.equal(err, null);
        t.deepEqual(resp, [{range : 5 }]);
        t.end();
    }

});

test('query - BETWEEN - list attributes - query filter match', function(t) {

    dyno.query({id:{'EQ':'yo'}, range:{'BETWEEN':[4,6]}},
    {filter:{extra:{EQ:'hi'}}}, itemResp);
    function itemResp(err, resp) {
        t.equal(err, null);
        t.deepEqual(resp, [{id : 'yo', range : 5, extra: 'hi' }]);
        t.end();
    }
});

test('query - BETWEEN - list attributes - query filter doesnt match', function(t) {

    dyno.query({id:{'EQ':'yo'}, range:{'BETWEEN':[4,6]}},
    {filter:{extra:{EQ:'hello'}}}, itemResp);
    function itemResp(err, resp) {
        t.equal(err, null);
        t.deepEqual(resp, []);
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
        if(item.range > 7) return t.end();
        item.range+=1;
        dyno.putItem(item, itemResp);
    }
});

test('scan - stream', function(t) {

    dyno.scan().pipe(es.writeArray(function(err, data){
        t.equal(err, null);
        t.equal(data.length, 4);
        t.deepEqual(data[0], {id: 'yo', range:5});
        t.deepEqual(data[3], {id: 'yo', range:8});
        t.end();
    }));

});

test('scan - callback', function(t) {

    dyno.scan(scanResp);
    function scanResp(err, items){
        t.equal(err, null);
        t.equal(items.length, 4);
        t.deepEqual(items[0], {id: 'yo', range:5});
        t.deepEqual(items[3], {id: 'yo', range:8});
        t.end();
    }

});

test('scan - callback, paging 2 pages', function(t) {

    dyno.scan({limit:1, pages:2}, scanResp);
    function scanResp(err, items){
        t.equal(err, null);
        t.equal(items.length, 2);
        t.deepEqual(items[0], {id: 'yo', range:5});
        t.deepEqual(items[1], {id: 'yo', range:6});
        t.end();
    }

});

test('query - stream', function(t) {

    var dr = dyno.query({id:{'EQ':'yo'}})
        .pipe(es.writeArray(function(err, data){
            t.equal(err, null);
            t.equal(data.length, 4);
            t.deepEqual(data[0], {id: 'yo', range:5});
            t.deepEqual(data[3], {id: 'yo', range:8});
            t.end();
        }));
});


test('query - stream, with paging', function(t) {

    var dr = dyno.query({id:{'EQ':'yo'}}, {limit:1})
        .pipe(es.writeArray(function(err, data){
            t.equal(err, null);
            t.equal(data.length, 4);
            t.deepEqual(data[0], {id: 'yo', range:5});
            t.deepEqual(data[3], {id: 'yo', range:8});
            t.end();
        }));
});

test('query - stream, paging get 2 pages', function(t) {

    var dr = dyno.query({id:{'EQ':'yo'}}, {limit:1, pages:2 })
        .pipe(es.writeArray(function(err, data){
            t.equal(err, null);
            t.equal(data.length, 2);
            t.deepEqual(data[0], {id: 'yo', range:5});
            t.deepEqual(data[1], {id: 'yo', range:6});
            t.end();
        }));
});

test('query - callback, paging get all pages', function(t) {

    var dr = dyno.query({id:{'EQ':'yo'}}, {limit:1, pages:0}, queryResp);

    function queryResp(err, items) {
        t.equal(err, null);
        t.equal(items.length, 4);
        t.deepEqual(items[0], {id: 'yo', range:5});
        t.deepEqual(items[3], {id: 'yo', range:8});
        t.end();
    }
});

test('query - callback, paging via prev/next', function(t) {

    dyno.query({id:{'EQ':'yo'}}, {limit:1, pages:1}, firstResp);

    function firstResp(err, items, metas) {
        t.equal(err, null);
        t.equal(items.length, 1);
        t.deepEqual(items[0], {id: 'yo', range:5});
        var next = metas.pop().last;
        t.ok(next, 'last evaluated key is not null');
        nextQuery(next);
    }

    function nextQuery(next) {
        dyno.query({id:{'EQ':'yo'}}, {start:next, limit:1, pages:2}, function(err, items) {
            t.equal(err, null);
            t.equal(items.length, 2);
            t.deepEqual(items[0], {id: 'yo', range:6});
            t.deepEqual(items[1], {id: 'yo', range:7});
            t.end();
        });
    }

});

test('teardown', s.teardown);

test('setup', s.setup());
test('setup table', s.setupTable);
test('setup items', function(t) {
    var item = {id: 'yo', range: 5, val: new Buffer('yep', 'utf8')};

    dyno.putItem(item, itemResp);
    function itemResp(err, resp) {
        if(item.range > 6) return t.end();
        item.range++;
        dyno.putItem(item, itemResp);
    }
});

test('getItem with buffer', function(t) {

    dyno.getItem({id:'yo', range:5}, function(err, data) {
        t.equal(err, null);
        t.deepEqual(data.val.toString('utf8'), 'yep');
        t.end();
    });

});

test('teardown', s.teardown);

test('setup', s.setup());
test('setup table', s.setupTable);
test('update', function(t) {
    var item = {put:{str: 'a', num: 12}, add:{count:1}};
    var key = {id:'yo', range:5};

    dyno.updateItem(key, item, itemResp);
    function itemResp(err, resp) {
        t.equal(err, null);
        t.deepEqual(resp, { id: 'yo', range: 5, str: 'a', num: 12, count: 1});
        dyno.getItem(key, getItem);
    }
    function getItem(err, resp) {
        t.equal(err, null);
        t.deepEqual(resp, {id: 'yo', range: 5, str: 'a', num:12, count:1});
        t.end();
    }
});
test('update - delete', function(t) {
    var item = {delete:['str','num']};
    var key = {id:'yo', range:5};

    dyno.updateItem(key, item, itemResp);
    function itemResp(err, resp) {
        t.equal(err, null);
        t.deepEqual(resp, {id: 'yo', range: 5, count:1});
        dyno.getItem(key, getItem);
    }
    function getItem(err, resp) {
        t.equal(err, null);
        t.deepEqual(resp, {id: 'yo', range: 5, count:1});
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
                "id" : "yo",
                "range" : 5,
                "newkey" : "hi",
            }, 'item was really updated');
            t.end();
        });
    });
});


test('update Item ', function(t) {

    var key = { id: 'yo', range: 5 };
    var actions = {put: { newset: ['a', 'b'] }, delete: ['newkey'], add: {counter: 1}};
    var d = dyno.updateItem(key, actions, function(err, resp){
        t.notOk(err);
        dyno.getItem(key, function(err, data){
            t.notOk(err, 'no error');
            t.deepEqual(data, {
                "id" : "yo",
                "range" : 5,
                "newset" : ['a', 'b'],
                "counter" : 1
            }, 'item was really updated');
            t.end();
        });
    });
});


test('update Item - delete from set', function(t) {

    var key = { id: 'yo', range: 5 };
    var actions = {delete: {newset: ['a'], 'counter':null}};
    var d = dyno.updateItem(key, actions, function(err, resp){
        t.notOk(err);
        dyno.getItem(key, function(err, data){
            t.notOk(err, 'no error');
            t.deepEqual(data, {
                "id" : "yo",
                "range" : 5,
                "newset" : ['b'],
            }, 'item was really updated');
            t.end();
        });
    });
});

test('update Item - with condition', function(t) {

    var key = { id: 'yo', range: 5 };

    var actions = {put: {anotherkey: 'hello'}, delete: ['otherrange'], add: {counter: 1}};
    var d = dyno.updateItem(key, actions, {
        expected: { 'range': {'EQ' : [5] }}
    }, function(err, resp){
        t.notOk(err);
        t.equal(resp.anotherkey, 'hello', 'item has new attribute');
        t.end();
    });
});


test('update Item - with NOT_NULL condition', function(t) {

    var key = { id: 'yo', range: 5 };

    var actions = {put: {anotherkey2: 'hello2'}, delete: ['otherrange'], add: {counter: 1}};
    var d = dyno.updateItem(key, actions, {
        expected: {
            'range': 'NOT_NULL',
            'id': {'EQ': 'yo'}
        }
    }, function(err, resp){
        t.notOk(err, 'no error');
        t.equal(resp.anotherkey2, 'hello2', 'item has new attribute');
        t.end();
    });
});

test('update Item - with condition. fail conditions', function(t) {

    var key = { id: 'yo', range: 5 };

    var actions = {put: {anotherkey: 'hello'}};
    var d = dyno.updateItem(key, actions, {
    expected: { 'anotherkey': { 'EQ' : ['hi'] }}
    }, function(err, resp){
        t.ok(err, 'should return error');
        t.equal(err.message, 'The conditional request failed');
        t.equal(err.code, 'ConditionalCheckFailedException');
        t.end();
    });
});

test('teardown', s.teardown);

test('setup', s.setup());
test('setup table', s.setupTable);

test('putItem return values', function(t) {
    var item = {id: 'yo', range: 5};

    dyno.putItem(item, { capacity: 'TOTAL' }, response);

    function response(err, result, meta) {
        t.ifError(err, 'completed request');
        if (err) return t.end();

        t.deepEqual(result, item, 'item matches result');
        t.ok(meta, 'metadata returned');
        t.ok(meta.capacity, 'capacity returned');
        t.end();
    }
});

test('updateItem return values', function(t) {
    var item = {id: 'yo', range: 5};
    var update = {put: { newkey: 'hi' }};

    dyno.updateItem(item, update, { capacity: 'TOTAL' }, response);

    function response(err, result, meta) {
        t.ifError(err, 'completed request');
        if (err) return t.end();

        t.deepEqual(result, _({newkey: 'hi'}).extend(item), 'updated item matches result');
        t.ok(meta, 'metadata returned');
        t.ok(meta.capacity, 'capacity returned');
        t.end();
    }
});

test('getItem return values', function(t) {
    var key = {id: 'yo', range: 5};

    dyno.getItem(key, { capacity: 'TOTAL' }, response);

    function response(err, result, meta) {
        t.ifError(err, 'completed request');
        if (err) return t.end();

        t.deepEqual(result, _({newkey: 'hi'}).extend(key), 'item matches result');
        t.ok(meta, 'metadata returned');
        t.ok(meta.capacity, 'capacity returned');
        t.end();
    }
});

test('teardown', s.teardown);
