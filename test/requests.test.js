var test = require('tape');
var testTables = require('./test-tables');
var dynamodb = require('dynamodb-test')(test, 'dyno', testTables.idhash);
var Requests = require('../lib/requests');
var _ = require('underscore');
var crypto = require('crypto');
var Dyno = require('..');
var queue = require('queue-async');

var fixtures = _.range(150).map(function(i) {
  return {
    id: i.toString(),
    data: crypto.randomBytes(5 * 1024)
  };
});

test('[requests] properties', function(assert) {
  var requests = Requests(dynamodb.dynamodb);
  assert.equal(typeof requests.batchWriteItemRequests, 'function', 'exposes batchWriteItemRequests function');
  assert.equal(typeof requests.batchGetItemRequests, 'function', 'exposes batchGetItemRequests function');
  assert.end();
});

dynamodb.start();

dynamodb.test('[requests] batchGetItemRequests (single table)', fixtures, function(assert) {
  var dyno = Dyno({
    table: dynamodb.tableName,
    region: 'local',
    endpoint: 'http://localhost:4567'
  });

  var params = { RequestItems: {} };
  params.RequestItems[dynamodb.tableName] = {
    Keys: _.range(150).map(function(i) {
      return { id: i.toString() };
    })
  };

  var found = dyno.batchGetItemRequests(params);
  assert.equal(found.length, 2, 'split 150 keys into two requests');
  queue()
    .defer(found[0].send.bind(found[0]))
    .defer(found[1].send.bind(found[1]))
    .awaitAll(function(err, results) {
      assert.ifError(err, 'requests were sent successfully');
      results = results[0].Responses[dynamodb.tableName].concat(results[1].Responses[dynamodb.tableName]);
      assert.equal(results.length, 150, 'all responses were recieved');
      assert.end();
    });
});

dynamodb.test('[requests] batchGetItemRequests.sendAll (single table)', fixtures, function(assert) {
  var dyno = Dyno({
    table: dynamodb.tableName,
    region: 'local',
    endpoint: 'http://localhost:4567'
  });

  var params = { RequestItems: {} };
  params.RequestItems[dynamodb.tableName] = {
    Keys: _.range(150).map(function(i) {
      return { id: i.toString() };
    })
  };

  var found = dyno.batchGetItemRequests(params);
  assert.equal(found.length, 2, 'split 150 keys into two requests');
  found.sendAll(function(err, results) {
    assert.ifError(err, 'requests were sent successfully');
    results = results[0].Responses[dynamodb.tableName].concat(results[1].Responses[dynamodb.tableName]);
    assert.equal(results.length, 150, 'all responses were recieved');
    assert.end();
  });
});

dynamodb.test('[requests] batchWriteItemRequests (single table, small writes)', fixtures, function(assert) {
  var dyno = Dyno({
    table: dynamodb.tableName,
    region: 'local',
    endpoint: 'http://localhost:4567'
  });

  var params = { RequestItems: {} };
  params.RequestItems[dynamodb.tableName] = [];

  _.range(200, 245).forEach(function(i) {
    params.RequestItems[dynamodb.tableName].push({
      PutRequest: { Item: { id: i.toString() } }
    });
  });
  _.range(150).forEach(function(i) {
    params.RequestItems[dynamodb.tableName].push({
      DeleteRequest: { Key: { id: i.toString() } }
    });
  });

  var found = dyno.batchWriteItemRequests(params);
  assert.equal(found.length, 8, 'split 150 deletes and 45 puts into 8 requests');
  var q = queue();
  found.forEach(function(req) {
    q.defer(req.send.bind(req));
  });
  q.awaitAll(function(err, results) {
    assert.ifError(err, 'requests were sent successfully');
    assert.deepEqual(results, [
      { UnprocessedItems: {} },
      { UnprocessedItems: {} },
      { UnprocessedItems: {} },
      { UnprocessedItems: {} },
      { UnprocessedItems: {} },
      { UnprocessedItems: {} },
      { UnprocessedItems: {} },
      { UnprocessedItems: {} }
    ], 'expected results from each request');
    dynamodb.dynamo.scan({
      TableName: dynamodb.tableName
    }, function(err, data) {
      if (err) throw err;
      assert.equal(data.Items.length, 45, '150 items deleted, 45 written');
      assert.end();
    });
  });
});

dynamodb.test('[requests] batchWriteItemRequests (single table, large writes)', function(assert) {
  var dyno = Dyno({
    table: dynamodb.tableName,
    region: 'local',
    endpoint: 'http://localhost:4567'
  });

  var params = { RequestItems: {} };
  params.RequestItems[dynamodb.tableName] = [];

  _.range(1, 25).forEach(function(i) {
    params.RequestItems[dynamodb.tableName].push({
      PutRequest: {
        Item: {
          id: i.toString(),
          data: crypto.randomBytes(690 * 1024)
        }
      }
    });
  });

  var found = dyno.batchWriteItemRequests(params);
  assert.equal(found.length, 2, 'split 25 large puts into 2 requests');
  // Test doesn't run the requests because dynalite barfs on items > 400KB
  assert.end();
});

dynamodb.close();
