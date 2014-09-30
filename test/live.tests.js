// Live tests create the table once and truncate it between runs
var s = require('./setup')(true);
require('./test.batch');
require('./test.convertTypes');
require('./test.item');
require('./test.throughput');

// Don't run table create/list/delete tests live.
// require('./test.table');

s.test('delete table', s.deleteTable);
