var test = require('tap').test;
var s = require('./setup');
var es = require('event-stream');
var seedrandom = require('seedrandom');
var dyno = s.dyno;

test('setup', s.setup());
test('setup table', s.setupTable);
test('putItems', function(t) {
    var items = randomItems(1000);

    dyno.putItems(items, itemResp);
    function itemResp(err, resp) {
        t.equal(err, null);
        t.end();
    }
});

test('deleteItems', function(t) {
    var itemIds = randomItems(1000).map(function(item){
        return { 
            id: item.id,
            range: item.range
        };
    });

    dyno.deleteItems(itemIds, itemResp);
    function itemResp(err, resp) {
        t.equal(err, null);
        t.end();
    }
});
test('teardown', s.teardown);

function randomItems(n) {
    var items = [];
    var rng = seedrandom('test')
    for (var i = 0; i < n; i++) {
        items.push({
            id: 'id:' + i.toString(),
            range: i,
            data: new Array(Math.round(rng() * 10000)).join(' '),
        });
    }
    return items;
}
