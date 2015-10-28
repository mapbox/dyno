var path = require('path');
var _ = require('underscore');
var test = require('tape');
var testTables = require('./test-tables');
var dynamodb = require('dynamodb-test')(test, 'dyno', testTables['idhash-range']);
var crypto = require('crypto');
var cli = path.resolve(__dirname, '..', 'bin', 'cli.js');
var exec = require('child_process').exec;
var Dyno = require('..');

// use --verbose to print cli output to terminal during the test run
var verbose = process.argv.indexOf('--verbose') > -1;

function randomItems(num) {
  return _.range(num).map(function(i) {
    return {
      id: i.toString(),
      range: i,
      data: crypto.randomBytes(36)
    };
  });
}

function runCli(params, callback) {
  var options = {
    env: _(process.env).clone(),
    cwd: path.resolve(__dirname, '..')
  };

  options.env.AWS_ACCESS_KEY_ID = 'fake';
  options.env.AWS_SECRET_ACCESS_KEY = 'fake';
  options.env.AWS_SESSION_TOKEN = 'fake';

  var cmd = _.union([cli], params).join(' ');
  var proc = exec(cmd, options, callback);

  if (verbose) {
    proc.stdout.pipe(process.stdout);
    proc.stderr.pipe(process.stderr);
  }

  return proc;
}

var expectedTable = {
  Table: {
    AttributeDefinitions: [
      { AttributeName: 'id', AttributeType: 'S' },
      { AttributeName: 'range', AttributeType: 'N' }
    ],
    TableArn: 'arn:aws:dynamodb:us-east-1:000000000000:table/' + dynamodb.tableName,
    TableName: dynamodb.tableName,
    ProvisionedThroughput: {
      WriteCapacityUnits: 1,
      ReadCapacityUnits: 1,
      NumberOfDecreasesToday: 0
    },
    KeySchema: [
      { AttributeName: 'id', KeyType: 'HASH' },
      { AttributeName: 'range', KeyType: 'RANGE' }
    ],
    CreationDateTime: '[TIMESTAMP]',
    ItemCount: 0,
    TableSizeBytes: 0,
    TableStatus: 'ACTIVE'
  }
};

var cleanedTable = _.omit(JSON.parse(JSON.stringify(expectedTable.Table)), [
  'CreationDateTime',
  'IndexSizeBytes',
  'IndexStatus',
  'ItemCount',
  'TableSizeBytes',
  'TableStatus',
  'LastDecreaseDateTime',
  'LastIncreaseDateTime'
]);

delete cleanedTable.ProvisionedThroughput.NumberOfDecreasesToday;

test('[cli] help', function(assert) {
  runCli(['--help'], function(err, stdout, stderr) {
    assert.ifError(err, 'cli success');
    assert.notOk(stdout, 'nothing logged to stdout');
    assert.ok(/Usage: dyno/.test(stderr), 'usage info printed to stderr');
    assert.end();
  });
});

test('[cli] invalid command', function(assert) {
  runCli(['ham', 'local/' + dynamodb.tableName], function(err, stdout, stderr) {
    assert.equal(err.code, 1, 'cli exit 1');
    assert.notOk(stdout, 'nothing logged to stdout');
    assert.ok(/Error: Use a valid sub-command/.test(stderr), 'error printed to stderr');
    assert.end();
  });
});

test('[cli] invalid command', function(assert) {
  runCli(['table'], function(err, stdout, stderr) {
    assert.equal(err.code, 1, 'cli exit 1');
    assert.notOk(stdout, 'nothing logged to stdout');
    assert.ok(/Error: Specify a region/.test(stderr), 'error printed to stderr');
    assert.end();
  });
});

test('[cli] no table specified', function(assert) {
  runCli(['table', 'local'], function(err, stdout, stderr) {
    assert.equal(err.code, 1, 'cli exit 1');
    assert.notOk(stdout, 'nothing logged to stdout');
    assert.ok(/Error: Specify a table name/.test(stderr), 'error printed to stderr');
    assert.end();
  });
});

dynamodb.test('[cli] list tables', function(assert) {
  runCli(['tables', 'local'], function(err, stdout) {
    assert.ifError(err, 'cli success');
    assert.ok((new RegExp(dynamodb.tableName)).test(stdout), 'printed table name');
    assert.end();
  });
});

dynamodb.test('[cli] describe table', function(assert) {
  runCli(['table', 'local/' + dynamodb.tableName], function(err, stdout) {
    assert.ifError(err, 'cli success');

    var found = JSON.parse(stdout.trim());
    found.Table.CreationDateTime = '[TIMESTAMP]';

    assert.deepEqual(found, expectedTable, 'expected table information printed to stdout');
    assert.end();
  });
});

dynamodb.test('[cli] export table', function(assert) {
  var records = randomItems(10);
  dynamodb.dyno.putItems(records, function(err) {
    if (err) {
      assert.ifError(err, 'failed to put records');
      return assert.end();
    }

    runCli(['export', 'local/' + dynamodb.tableName], function(err, stdout) {
      assert.ifError(err, 'cli success');

      var results = stdout.trim().split('\n');

      var table = JSON.parse(results.shift());

      assert.deepEqual(table, cleanedTable, 'printed cleaned table definition to stdout');

      var expectedRecords = records.map(function(record) {
        return Dyno.serialize(record);
      });

      var intersection = _.intersection(expectedRecords, results);
      assert.equal(intersection.length, results.length, 'printed records to stdout');
      assert.end();
    });
  });
});

dynamodb.test('[cli] scan table', function(assert) {
  var records = randomItems(10);
  dynamodb.dyno.putItems(records, function(err) {
    if (err) {
      assert.ifError(err, 'failed to put records');
      return assert.end();
    }

    runCli(['scan', 'local/' + dynamodb.tableName], function(err, stdout) {
      assert.ifError(err, 'cli success');

      var results = stdout.trim().split('\n');

      var expectedRecords = records.map(function(record) {
        return Dyno.serialize(record);
      });

      var intersection = _.intersection(expectedRecords, results);
      assert.equal(intersection.length, results.length, 'printed records to stdout');
      assert.end();
    });
  });
});

dynamodb.delete();

test('[cli] import table', function(assert) {
  var records = randomItems(10);
  var serialized = _.union([cleanedTable], records).map(function(line) {
    return Dyno.serialize(line);
  }).join('\n');

  var proc = runCli(['import', 'local/' + dynamodb.tableName], function(err) {
    assert.ifError(err, 'cli success');
    dynamodb.dyno.scan({ pages: 0 }, function(err, items) {
      if (err) {
        assert.ifError(err, 'failed to import table + data');
        return assert.end();
      }

      var expectedRecords = records.map(function(record) {
        return Dyno.serialize(record);
      });

      items = items.map(function(record) {
        return Dyno.serialize(record);
      });

      var intersection = _.intersection(expectedRecords, items);
      assert.equal(intersection.length, items.length, 'loaded records into table');
      assert.end();
    });
  });

  proc.stdout.pipe(process.stdout);
  proc.stderr.pipe(process.stderr);
  proc.stdin.write(serialized);
  proc.stdin.end();
});

dynamodb.test('[cli] import data', function(assert) {
  var records = randomItems(10);
  var serialized = records.map(function(line) {
    return Dyno.serialize(line);
  }).join('\n');

  var proc = runCli(['put', 'local/' + dynamodb.tableName], function(err) {
    assert.ifError(err, 'cli success');
    dynamodb.dyno.scan({ pages: 0 }, function(err, items) {
      if (err) {
        assert.ifError(err, 'failed to import table + data');
        return assert.end();
      }

      var expectedRecords = records.map(function(record) {
        return Dyno.serialize(record);
      });

      items = items.map(function(record) {
        return Dyno.serialize(record);
      });

      var intersection = _.intersection(expectedRecords, items);
      assert.equal(intersection.length, items.length, 'loaded records into table');
      assert.end();
    });
  });

  proc.stdin.write(serialized);
  proc.stdin.end();
});

dynamodb.test('[cli] export complicated record', function(assert) {
  var record = {
    id: 'id:1',
    range: 1,
    buffer: new Buffer('hello world!'),
    array: [0, 1, 2],
    newline: '0\n1'
  };

  dynamodb.dyno.putItem(record, function(err) {
    if (err) {
      assert.ifError(err, 'failed to put record');
      return assert.end();
    }

    runCli(['scan', 'local/' + dynamodb.tableName], function(err, stdout) {
      assert.ifError(err, 'cli success');
      assert.equal(stdout.trim(), '{"id":{"S":"id:1"},"range":{"N":"1"},"buffer":{"B":"aGVsbG8gd29ybGQh"},"array":{"L":[{"N":"0"},{"N":"1"},{"N":"2"}]},"newline":{"S":"0\\n1"}}', 'printed record to stdout');
      assert.end();
    });
  });
});

dynamodb.test('[cli] import complicated record', function(assert) {
  var expected = {
    id: 'id:1',
    range: 1,
    buffer: new Buffer('hello world!'),
    array: [0, 1, 2],
    newline: '0\n1'
  };

  var proc = runCli(['put', 'local/' + dynamodb.tableName], function(err) {
    assert.ifError(err, 'cli success');

    dynamodb.dyno.scan(function(err, items) {
      if (err) {
        assert.ifError(err, 'failed to scan table');
        return assert.end();
      }
      assert.equal(items.length, 1, 'loaded one record');
      assert.deepEqual(items[0], expected, 'loaded expected record');
      assert.end();
    });
  });

  proc.stdin.write('{"id":{"S":"id:1"},"range":{"N":"1"},"buffer":{"B":"aGVsbG8gd29ybGQh"},"array":{"L":[{"N":"0"},{"N":"1"},{"N":"2"}]},"newline":{"S":"0\\n1"}}');
  proc.stdin.end();
});

dynamodb.close();
