var s = require('./setup')();
var test = s.test;
var es = require('event-stream');
var dyno = s.dyno;
var queue = require('queue-async');

test('setup', s.setup());
test('setup table', s.setupTable);
test('scan (complete)', function(t) {
    t.plan(1);
    var itemCount = 100;
    var foundItems = 0;
    var itemKeys = [];

    var thru = es.through(
        function item(doc) {
            if (itemKeys.indexOf(doc.id) !== -1)
                foundItems++;
        },
        function done() {
            t.equal(foundItems, itemCount, 'Scanned all items');
        }
    );

    var q = queue(1);
    for (var i = 0; i < itemCount; i++) {
        var itemID = 'item-' + i;
        itemKeys.push(itemID);
        q.defer(dyno.putItem, {id: itemID, range: i });
    }
    q.awaitAll(function() {
        dyno.scan().pipe(thru);
    });
});

// note: dynalite does not currently support FilterExpression
/*
test('scan (partial, id < 50)', function(t) {
    var foundItems = 0;
    var thru = es.through(
        function item(doc) {
            foundItems++;
        },
        function done() {
            t.equal(foundItems, 50, 'Scanned all items');
            t.end();
        }
    );
    dyno.scan({filter: 'id < 50'}).pipe(thru);
});
*/

test('teardown', s.teardown);
