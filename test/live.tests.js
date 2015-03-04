// Live tests create the table once and truncate it between runs
var s = require('./setup')(true);
require('./test.batch');

// No need to run type conversion tests that don't touch DynamoDB
// require('./test.convertTypes');

require('./test.item');
require('./test.throughput');

// Don't run table create/list/delete tests live.
// require('./test.table');

s.test('delete table', function(t) {
    setTimeout(function() {
        s.deleteTable(t);
    }, 1000);
});
