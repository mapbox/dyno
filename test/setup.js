var fixtures = require('./fixtures');
var Dynalite = require('dynalite');
var dynalite;

var dyno = module.exports.dyno = require('../')({
    accessKeyId: 'fake',
    secretAccessKey: 'fake',
    region: 'us-east-1',
    table: 'test',
    endpoint: 'http://localhost:4567'
});

module.exports.setup = function(opts) {
    if(!opts) opts = {};
    return function(t) {
        dynalite = Dynalite({
            createTableMs: opts.createTableMs || 0,
            updateTableMs: opts.updateTableMs || 0,
            deleteTableMs: opts.deleteTableMs || 0
        });
        dynalite.listen(4567, function() {
            t.end();
        });
    }
}

module.exports.setupTable = function(t) {
    dyno.createTable(fixtures.test, function(err, resp){
        if(err) throw err;
        t.end();
    });
}

module.exports.teardown = function(t) {
    dynalite.close();
    t.end();
}
