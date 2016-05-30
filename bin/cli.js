#!/usr/bin/env node
var params = require('minimist')(process.argv.slice(2));
var Dyno = require('../index.js');
var queue = require('queue-async');
var es = require('event-stream');
var stream = require('stream');

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

params.region = params._[1] ? params._[1].split('/')[0] : null;
if (!params.region) {
  console.error('Error: Specify a region');
  usage();
  process.exit(1);
}
if (params.region === 'local') params.endpoint = 'http://localhost:4567';
if (params.e) params.endpoint = params.e;

params.table = params._[1] ? params._[1].split('/')[1] : null;
if (!params.table && params.command !== 'tables') {
  console.error('Error: Specify a table name');
  usage();
  process.exit(1);
}

if (!params.table && params.command === 'tables') {
  params.table = 'none';
}

var dyno = Dyno(params);

// Transform stream to stringifies JSON objects and base64 encodes buffers
function Stringifier() {
  var stringifier = new stream.Transform({ highWaterMark: 100 });
  stringifier._writableState.objectMode = true;
  stringifier._readableState.objectMode = false;

  stringifier._transform = function(record, enc, callback) {
    var str = Dyno.serialize(record);

    this.push(str + '\n');
    setImmediate(callback);
  };

  return stringifier;
}

// Transform stream parses JSON strings and base64 decodes into buffers
function Parser() {
  var parser = new stream.Transform({ highWaterMark: 100 });
  parser._writableState.objectMode = false;
  parser._readableState.objectMode = true;

  var firstline = true;
  parser._transform = function(record, enc, callback) {
    if (!record || record.length === 0) return;

    if (firstline) {
      firstline = false;

      var parsed = Dyno.deserialize(record);
      if (!Object.keys(parsed).every(function(key) {
        return !!parsed[key];
      })) return this.push(JSON.parse(record.toString()));
    }

    record = Dyno.deserialize(record);

    this.push(record);
    setImmediate(callback);
  };

  return parser;
}

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
  dyno.scanStream()
    .pipe(Stringifier())
    .pipe(process.stdout)
    .on('error', function(err) {
      console.error(err);
      process.exit(1);
    });
}

// Transform stream that aggregates into sets of 25 objects
function Aggregator(withTable) {
  var firstline = !!withTable;

  var aggregator = new stream.Transform({ objectMode: true, highWaterMark: 100 });
  aggregator.records = [];

  aggregator._transform = function(record, enc, callback) {
    if (!record) return;
    if (firstline) {
      firstline = false;
      this.push(record);
    } else if (aggregator.records.length === 25) {
      this.push(aggregator.records);
      aggregator.records = [record];
    } else {
      aggregator.records.push(record);
    }
    callback();
  };

  aggregator._flush = function(callback) {
    if (aggregator.records.length) this.push(aggregator.records);
    callback();
  };

  return aggregator;
}

function Importer(withTable) {
  var firstline = !!withTable;
  var q = queue(10);

  var importer = stream.Transform({ objectMode: true, highWaterMark: 100 });
  var queued = 0;

  importer._transform = function(data, enc, callback) {
    if (!data) return;
    if (queued > 100)
      setImmediate(importer._transform.bind(importer), data, enc, callback);

    if (firstline) {
      firstline = false;
      this.pause();
      data.TableName = params.table;
      delete data.TableArn;
      dyno.createTable(data, function(err) {
        if (err) throw err;
        this.resume();
      }.bind(this));
    } else {
      var reqs = { RequestItems: {} };
      reqs.RequestItems[params.table] = [];

      data.forEach(function(item) {
        reqs.RequestItems[params.table].push({
          PutRequest: { Item: item }
        });
      });

      dyno.batchWriteItemRequests(reqs).forEach(function(req) {
        queued++;
        q.defer(function(next) {
          req.send(function(err) {
            queued--;
            next(err);
          });
        });
      });
      callback();
    }
  };

  importer._flush = function(callback) {
    q.awaitAll(callback);
  };

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
  data.TableNames.forEach(function(name) {
    console.log(name);
  });
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
    .pipe(Parser())
    .pipe(Aggregator(true))
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
    .pipe(Parser())
    .pipe(Aggregator(false))
    .pipe(Importer(false))
    .on('error', function(err) {
      console.error(err);
      process.exit(1);
    });
}
