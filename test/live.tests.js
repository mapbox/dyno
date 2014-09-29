// Live tests create the table once and truncate it between runs
var s = require('./setup')(true);
require('./test.batch');
require('./test.convertTypes');
require('./test.item');

// Don't run table create/list/delete tests live.
// require('./test.table');

var test = require('tap').test;

test('delete Table', { timeout: Infinity }, function(t) {
    s.dyno.deleteTable(s.tableName, function(err, resp){
        t.ifError(err, 'deleted table ' + s.tableName);
        t.deepEqual(resp, {});
        t.end();
    });
});
