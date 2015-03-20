#!/usr/bin/env node
var argv = require('minimist')(process.argv.slice(2));
var Dyno = require('../index.js');
var queue = require('queue-async');
var es = require('event-stream');
var _ = require('underscore');

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
            .pipe(es.stringify())
            .pipe(process.stdout)
            .on('error', function(err) {
                error(err);
            })
            .on('end', process.exit);
    }

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
        .pipe(es.stringify())
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
            .pipe(es.parse())
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
            .pipe(es.parse())
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

    if (argv._[1] === 'query') {
        dyno.describeTable(doQuery);

        function doQuery(err, table) {
            if (err) error(err);
            var query = {};
            var hashkey = _(table.Table.KeySchema).findWhere({ KeyType: 'HASH' }).AttributeName;
            var rangekey = _(table.Table.KeySchema).findWhere({ KeyType: 'RANGE' });
            if (rangekey) rangekey = rangekey.AttributeName;

            query[hashkey] = { EQ: argv._[3] };
            if(argv._[4]) query[rangekey] = { EQ: argv._[4] };
            dyno.query(query, argv, output);
        }
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
