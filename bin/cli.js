#!/usr/bin/env node
var argv = require('minimist')(process.argv.slice(2));
var Dyno = require('../index.js');
var queue = require('queue-async');
var es = require('event-stream');

process.env.AWS_ACCESS_KEY_ID = process.env.AWS_ACCESS_KEY_ID || 'fake';
process.env.AWS_SECRET_ACCESS_KEY = process.env.AWS_SECRET_ACCESS_KEY || 'fake';

(function() {
    var params = {};

    params.region = argv._[0];
    if (argv._[0] === 'local') params.endpoint = 'http://localhost:4567';
    if (argv.e) params.endpoint = argv.e;

    if (argv._[1] === 'tables') {
        dyno = Dyno(params);
        return dyno.listTables(output);
    }

    if (!argv._[2]) error('No table set');
    params.table = argv._[2];

    dyno = Dyno(params);

    function describedTable(err, resp) {
        if (err) return error(err);

        var deleteAttributes = [
            'CreationDateTime',
            'IndexSizeBytes',
            'IndexStatus',
            'ItemCount',
            'NumberOfDecreasesToday',
            'TableSizeBytes',
            'TableStatus',
            'LastDecreaseDateTime',
            'LastIncreaseDateTime'
        ];

        function replacer(key, value) {
            if (deleteAttributes.indexOf(key) !== -1) {
                return undefined;
            }
            return value;
        }

        console.log(JSON.stringify(resp.Table, replacer));
        dyno.scan()
            .pipe(stringifier)
            .pipe(process.stdout)
            .on('error', function(err) {
                error(err);
            })
            .on('end', process.exit);
    }

    // Stringifies JSON objects and base64 encodes buffers
    var stringifier = es.through(function(record) {
        this.emit('data', JSON.stringify(record, function(key) {
            var val = this[key];
            if (Buffer.isBuffer(val)) return 'base64:' + val.toString('base64');
            return val;
        }) + '\n');
    });

    // Parses JSON strings and base64 decodes into buffers
    var parser = es.through(function(record) {
        record = JSON.parse(record);

        var val;
        for (var key in record) {
            val = record[key];
            if (typeof val === 'string' && val.indexOf('base64:') === 0)
                record[key] = new Buffer(val.split('base64:').pop(), 'base64');
        }

        this.emit('data', record);
    });
    // dyno table -t
    if (argv._[1] === 'table') {
        return dyno.describeTable(output);
    }

    //describes the table, then scans and outputs all the data.
    if (argv._[1] === 'export') {
        dyno.describeTable(describedTable);
    }

    if (argv._[1] === 'scan') {
        dyno.scan()
        .pipe(stringifier)
        .pipe(process.stdout)
        .on('error', function(err) {
            error(err);
        })
        .on('end', process.exit);
    }

    //describes the table, then scans and outputs all the data.
    if (argv._[1] === 'import') {
        var importQueue = queue(10);
        var firstline = true;
        process.stdin
            .pipe(es.split())
            .pipe(parser)
            .pipe(es.through(function(data) {
                if (firstline) {
                    firstline = false;
                    this.pause();
                    data.TableName = params.table;
                    dyno.createTable(data, function(err) {
                        if (err) error(err);
                        this.resume();
                    }.bind(this));
                } else {
                    importQueue.defer(dyno.putItem, data);
                }
            }))
            .on('error', function(err) {
                error(err);
            });
        importQueue.awaitAll(function(err) {
            if (err) error(err);
        });
    }

    if (argv._[1] === 'put') {
        var putQueue = queue(10);
        process.stdin
            .pipe(es.split())
            .pipe(parser)
            .pipe(es.through(function(data) {
                putQueue.defer(dyno.putItem, data);
            }))
            .on('error', function(err) {
                error(err);
            });
        putQueue.awaitAll(function(err) {
            if (err) error(err);
        });
    }

})();

function output(err, resp) {
    if (err) console.error(err);
    if (resp) console.log(JSON.stringify(resp));
    process.exit(0);
}
function error(msg) {
    console.error(msg);
    process.exit(1);
}
