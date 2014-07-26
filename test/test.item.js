var test = require('tap').test;
var s = require('./setup');
var es = require('event-stream');
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
    function scanResp(err, resp){
        t.equal(err, null);
        t.equal(resp.items.length, 4);
        t.deepEqual(resp.items[0], {id: 'yo', range:5});
        t.deepEqual(resp.items[3], {id: 'yo', range:8});
        t.end();
    }

});

test('scan - callback, paging 2 pages', function(t) {

    dyno.scan({limit:1, pages:2}, scanResp);
    function scanResp(err, resp){
        t.equal(err, null);
        t.equal(resp.items.length, 2);
        t.deepEqual(resp.items[0], {id: 'yo', range:5});
        t.deepEqual(resp.items[1], {id: 'yo', range:6});
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

    var dr = dyno.query({id:{'EQ':'yo'}}, {limit:1, pages:0}, queryResp)

    function queryResp(err, resp) {
        t.equal(err, null);
        t.equal(resp.items.length, 4);
        t.deepEqual(resp.items[0], {id: 'yo', range:5});
        t.deepEqual(resp.items[3], {id: 'yo', range:8});
        t.end();
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
        t.deepEqual(data.Item.val.toString('utf8'), 'yep');
        t.end();
    });

});

test('teardown', s.teardown);
