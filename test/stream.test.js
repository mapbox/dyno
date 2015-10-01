var test = require('tape');
var testTables = require('./test-tables');
var scanTable = require('dynamodb-test')(test, 'dyno', testTables.idhash);
var queryTable = require('dynamodb-test')(test, 'dyno', testTables['idhash-numrange']);
var liveTable = require('dynamodb-test')(test, 'dyno', testTables.idhash, 'us-east-1');
var Dyno = require('..');
var Stream = require('../lib/stream');
var _ = require('underscore');
var crypto = require('crypto');

var fixtures = _.range(2345).map(function(i) {
  return {
    id: i.toString(),
    data: crypto.randomBytes(5 * 1024)
  };
});

var queryFixtures = _.range(2345).map(function(i) {
  return {
    id: 'id',
    num: i,
    data: crypto.randomBytes(5 * 1024)
  };
});

scanTable.start();

test('[stream] properties', function(assert) {
  var stream = Stream(scanTable.dynamo);
  assert.equal(typeof stream.query, 'function', 'exposes query function');
  assert.equal(typeof stream.scan, 'function', 'exposes scan function');
  assert.end();
});

scanTable.test('[stream] scan', fixtures, function(assert) {
  var dyno = Dyno({
    table: scanTable.tableName,
    region: 'local',
    endpoint: 'http://localhost:4567'
  });

  var count = 0;

  var scan = dyno.scanStream();
  scan
    .on('error', function(err) { assert.ifError(err, 'should not error'); })
    .on('data', function(item) {
      count++;
      assert.ok(item.id, 'streamed record has id');
      assert.equal(item.data.length, 5 * 1024, 'streamed record has correct buffer length');
      assert.ok(count <= 2345, 'streamed ' + count + ' records...');
      if (count > 2345) {
        scan.pause();
        assert.end();
      }
    })
    .on('end', function() {
      assert.equal(count, 2345, 'scanned all records');
      assert.end();
    });
});

queryTable.start();

queryTable.test('[stream] query', queryFixtures, function(assert) {
  var dyno = Dyno({
    table: queryTable.tableName,
    region: 'local',
    endpoint: 'http://localhost:4567'
  });

  var count = 0;

  var query = dyno.queryStream({
    KeyConditions: {
      id: {
        ComparisonOperator: 'EQ',
        AttributeValueList: ['id']
      }
    }
  });

  query
    .on('error', function(err) { assert.ifError(err, 'should not error'); })
    .on('data', function(item) {
      count++;
      assert.ok(item.id, 'queried record has id');
      assert.equal(item.data.length, 5 * 1024, 'queried record has correct buffer length');
      assert.ok(count <= 2345, 'queried ' + count + ' records...');
      if (count > 2345) {
        query.pause();
        assert.end();
      }
    })
    .on('end', function() {
      assert.equal(count, 2345, 'scanned all records');
      assert.end();
    });
});

queryTable.delete();
scanTable.close();

liveTable.start();

liveTable.test('[stream] live capacity consumption', fixtures.slice(0, 20), function(assert) {
  var dyno = Dyno({
    table: liveTable.tableName,
    region: 'us-east-1'
  });

  var count = 0;

  var scan = dyno.scanStream({ ReturnConsumedCapacity: 'TOTAL' });
  scan
    .on('error', function(err) { assert.ifError(err, 'should not error'); })
    .on('data', function() {
      count++;
      if (count > 20) {
        scan.pause();
        assert.end();
      }
    })
    .on('end', function() {
      assert.equal(count, 20, 'scanned all records');
      assert.deepEqual(scan.ConsumedCapacity, {
        TableName: liveTable.tableName,
        CapacityUnits: 13
      }, 'returns consumed capacity');
      assert.end();
    });
});

liveTable.delete();
