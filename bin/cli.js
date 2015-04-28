#!/usr/bin/env node
var params = require('minimist')(process.argv.slice(2));
var Dyno = require('../index.js');
var queue = require('queue-async');
var es = require('event-stream');

process.env.AWS_ACCESS_KEY_ID = process.env.AWS_ACCESS_KEY_ID || 'fake';
process.env.AWS_SECRET_ACCESS_KEY = process.env.AWS_SECRET_ACCESS_KEY || 'fake';

function usage() {
    console.error('');
    console.error('Usage: dyno <sub-command> <region>[/<tablename>]');
    console.error('');
    console.error('Valid sub-commands:');
    console.error(' - tables: list available tables');
    console.error(' - table: describe a single table');
    console.error(' - export: print table description and data to stdout');
    console.error(' - import: read table description and data into a new table');
    console.error(' - scan: print data to stdout');
    console.error(' - put: read data into an existing table');
    console.error('');
    console.error('Options:');
    console.error(' - e | endpoint: endpoint for DynamoDB. Automatically set to http://localhost:4567 if region is `local`');
    console.error('');
    console.error('Examples:');
    console.error('');
    console.error('dyno scan us-east-1/my-table');
    console.error('');
    console.error('dyno export us-east-1/my-table | dyno import local/my-local-copy');
}

if (params.help) {
    usage();
    process.exit(0);
}

params.command = params._[0];
var commands = ['tables', 'table', 'export', 'import', 'scan', 'put'];
if (commands.indexOf(params.command) === -1) {
    console.error('Error: Use a valid sub-command. One of ' + commands.join(', '));
    usage();
    process.exit(1);
}

params.region = params._[1].split('/')[0];
if (params.region === 'local') params.endpoint = 'http://localhost:4567';
if (params.e) params.endpoint = params.e;

params.table = params._[1].split('/')[1];
if (!params.table && params.command !== 'tables') {
    console.error('Error: Specify a table name');
    usage();
    process.exit(1);
}

var dyno = Dyno(params);

// Transform stream to stringifies JSON objects and base64 encodes buffers
var stringifier = es.through(function(record) {
    var str = JSON.stringify(record, function(key, value) {
        var val = this[key];
        if (Buffer.isBuffer(val)) return 'base64:' + val.toString('base64');
        return value;
    });

    this.emit('data', str + '\n');
});

// Transform stream parses JSON strings and base64 decodes into buffers
var parser = es.through(function(record) {
    if (!record) return;

    record = JSON.parse(record);

    var val;
    for (var key in record) {
        val = record[key];
        if (typeof val === 'string' && val.indexOf('base64:') === 0)
            record[key] = new Buffer(val.split('base64:').pop(), 'base64');
    }

    this.emit('data', record);
});

// Remove unimportant table metadata from the description
function cleanDescription(desc) {
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

    return JSON.stringify(desc.Table, function(key, value) {
        if (deleteAttributes.indexOf(key) !== -1) {
            return undefined;
        }
        return value;
    });
}

function scan() {
    dyno.scan()
        .pipe(stringifier)
        .pipe(process.stdout)
        .on('error', function(err) {
            console.error(err);
            process.exit(1);
        });
}

function Importer(withTable) {
    var firstline = !!withTable;
    var q = queue(10);

    var importer = es.through(function(data) {
        if (firstline) {
            firstline = false;
            this.pause();
            data.TableName = params.table;
            dyno.createTable(data, function(err) {
                if (err) error(err);
                this.resume();
            }.bind(this));
        } else {
            q.defer(dyno.putItem, data);
        }
    });

    q.awaitAll(function(err) {
        if (err) importer.emit('error', err);
    });

    return importer;
}

// ----------------------------------
// List tables
// ----------------------------------
if (params.command === 'tables') dyno.listTables(function(err, data) {
    if (err) {
        console.error(err);
        process.exit(1);
    }
    console.log(JSON.stringify(data));
});

// ----------------------------------
// Describe table
// ----------------------------------
if (params.command === 'table') dyno.describeTable(function(err, data) {
    if (err) {
        console.error(err);
        process.exit(1);
    }
    console.log(JSON.stringify(data));
});

// ----------------------------------
// Export table
// ----------------------------------
if (params.command === 'export') {
    return dyno.describeTable(function(err, desc) {
        if (err) {
            console.error(err);
            process.exit(1);
        }

        console.log(cleanDescription(desc));
        scan();
    });
}

// ----------------------------------
// Scan table
// ----------------------------------
if (params.command === 'scan') scan();

// ----------------------------------
// Import table
// ----------------------------------
if (params.command === 'import') {
    process.stdin
        .pipe(es.split())
        .pipe(parser)
        .pipe(Importer(true))
        .on('error', function(err) {
            console.error(err);
            process.exit(1);
        });
}

// ----------------------------------
// Import data
// ----------------------------------
if (params.command === 'put') {
    process.stdin
        .pipe(es.split())
        .pipe(parser)
        .pipe(Importer(false))
        .on('error', function(err) {
            console.error(err);
            process.exit(1);
        });
}
