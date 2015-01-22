#!/usr/bin/env node
var argv = require('minimist')(process.argv.slice(2));;
var Dyno = require('../index.js');
var queue = require('queue-async');
var es = require('event-stream');

(function() {
    var params = {};
    if (argv.e) params.endpoint = argv.e;
    if (argv.r) params.region = argv.r;

    if (argv._[0] === 'tables') {
        dyno = Dyno(params);
        return dyno.listTables(output);
    }

    if (!argv._[1]) error('No table set');
    params.table = argv._[1];

    dyno = Dyno(params);

    // dyno describe -t
    if (argv._[0] === 'describe') {
        return dyno.describeTable(output);
    }



    //describes the table, then scans and outputs all the data.
    if(argv._[0] === 'export') {
        dyno.describeTable(describedTable);
        function describedTable(err, resp){
            if(err) return error(err);
            console.log(resp);
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

    if(argv._[0] === 'put') {
        process.stdin
            .pipe(es.split())
            .pipe(dyno.putItem())
    }


})();

function output(err, resp) {
    if(err) console.error(err);
    if(resp) console.log(resp);
    process.exit(0);
}
function error(msg){
    console.error(msg);
    process.exit(1);
}
