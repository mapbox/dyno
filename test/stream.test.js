var test = require('tape');
var testTables = require('./test-tables');
var putTable = require('@mapbox/dynamodb-test')(test, 'dyno', testTables.idhash);
var scanTable = require('@mapbox/dynamodb-test')(test, 'dyno', testTables.idhash);
var queryTable = require('@mapbox/dynamodb-test')(test, 'dyno', testTables['idhash-numrange']);
var liveTable = require('@mapbox/dynamodb-test')(test, 'dyno', testTables.idhash, 'us-east-1');
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
  assert.equal(typeof stream.put, 'function', 'exposes put function');
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
      if (!item.id) assert.fail('stream record has no id');
      if (item.data.length !== 5 * 1024) assert.fail('streamed record has incorrect buffer length');
      if (count > 2345) {
        assert.fail('streamed too many records');
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
      if (!item.id) assert.fail('stream record has no id');
      if (item.data.length !== 5 * 1024) assert.fail('streamed record has incorrect buffer length');
      if (count > 2345) {
        assert.fail('streamed too many records');
        query.pause();
        assert.end();
      }
    })
    .on('end', function() {
      assert.equal(count, 2345, 'query stream returned all records');
      assert.end();
    });
});

queryTable.delete();

putTable.start();

putTable.test('[stream] put', function(assert) {
  var dyno = Dyno({
    table: putTable.tableName,
    region: 'local',
    endpoint: 'http://localhost:4567'
  });

  dyno.listTables(function(err) {
    if (err) return assert.end(err);

    var stream = dyno.putStream();
    for (var i = 0; i < fixtures.length; i++) {
      stream.write(fixtures[i]);
    }

    var count = 0;
    stream.on('finish', function() {
      dyno.scanStream({ ReturnConsumedCapacity: 'TOTAL' })
        .on('error', function(err) { assert.ifError(err, 'should not error'); })
        .on('data', function() {
          count++;
        })
        .on('end', function() {
          assert.equal(count, fixtures.length, 'wrote all features');
          assert.end();
        });
    }).end();
  });
});

putTable.delete();

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
