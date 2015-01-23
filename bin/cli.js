#!/usr/bin/env node
var argv = require('minimist')(process.argv.slice(2));;
var Dyno = require('../index.js');
var queue = require('queue-async');
var es = require('event-stream');

(function() {
    var params = {};
    if (argv.e) params.endpoint = argv.e;
    if (process.env.AWS_DEFAULT_REGION) params.region = process.env.AWS_DEFAULT_REGION;
    if (argv.r) params.region = argv.r;

    if (argv._[0] === 'tables') {
        dyno = Dyno(params);
        return dyno.listTables(output);
    }

    if (!argv._[1]) error('No table set');
    params.table = argv._[1];

    dyno = Dyno(params);

    // dyno table -t
    if (argv._[0] === 'table') {
        return dyno.describeTable(output);
    }



    //describes the table, then scans and outputs all the data.
    if(argv._[0] === 'export') {
        dyno.describeTable(describedTable);
        function describedTable(err, resp){
            if(err) return error(err);

            var deleteAttributes = [
                'CreationDateTime',
                'IndexSizeBytes',
                'IndexStatus',
                'ItemCount',
                'NumberOfDecreasesToday',
                'TableSizeBytes',
                'TableStatus'
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
                .on('error', function(err){
                    error(err);
                })
                .on('end', process.exit);
        }
    }

    if(argv._[0] === 'scan') {
        dyno.scan()
        .pipe(es.stringify())
        .pipe(process.stdout)
        .on('error', function(err){
            error(err);
        })
        .on('end', process.exit);
    }

    //describes the table, then scans and outputs all the data.
    if(argv._[0] === 'import') {
        var q = queue(10);
        var firstline = true;
        process.stdin
            .pipe(es.split())
            .pipe(es.parse())
            .pipe(es.through(function(data) {
                if(firstline) {
                    firstline = false;
                    this.pause();
                    data.TableName = params.table;
                    dyno.createTable(data, function(err) {
                        if(err) error(err);
                        this.resume();
                    }.bind(this));
                } else {
                    q.defer(dyno.putItem, data, {raw:true});
                }
            }))
            .on('error', function(err){
                error(err);
            })
        q.awaitAll(function(err) {
            if (err) error(err);
        });
    }

    if(argv._[0] === 'put') {
        var q = queue(10);
        process.stdin
            .pipe(es.split())
            .pipe(es.parse())
            .pipe(es.through(function(data) {
                q.defer(dyno.putItem, data);
            }));
        q.awaitAll(function(err) {
            if (err) error(err);
        });
    }


})();

function output(err, resp) {
    if(err) console.error(err);
    if(resp) console.log(JSON.stringify(resp));
    process.exit(0);
}
function error(msg){
    console.error(msg);
    process.exit(1);
}
