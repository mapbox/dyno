var test = require('tap').test;
var s = require('./setup');
var es = require('event-stream');
var dyno = s.dyno;

test('setup', s.setup());
test('setup table', s.setupTable);
test('putItems', function(t) {
    var items = randomItems(1000);

    dyno.putItems(items, itemResp);
    function itemResp(err, resp) {
        t.equal(err, null);
        t.deepEqual(resp, {
            UnprocessedItems: {}
        });
        t.end();
        // dyno.getItem(item, getItem);
    }
    function getItem(err, resp) {
        // t.equal(err, null);
        // t.deepEqual(resp, {Item:{id: 'yo', range: 5}});
        // t.end();
    }
});
test('teardown', s.teardown);

function randomItems(n) {
    var items = [];
    for (var i = 0; i < n; i++) {
        items.push({
            id: 'id:' + i.toString(),
            range: i,
            data: (new Array(10000)).join(' '),
        });
    }
    return items;
}
