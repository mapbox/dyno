var AWS = require('aws-sdk');
var test = require('tape');
var testTables = require('./test-tables');
var dynamodb = require('dynamodb-test')(test, 'dyno', testTables.idhash);
var second = require('dynamodb-test')(test, 'dyno', testTables.idhash);
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
second.start();

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

dynamodb.test('[requests] sendAll (single table)', fixtures, function(assert) {
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

    found.sendAll(4, function(err) {
      assert.ifError(err, 'can set concurrency');
      assert.end();
    });
  });
});

second.load(fixtures);

dynamodb.test('[requests] sendAll (two tables)', fixtures, function(assert) {
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
  params.RequestItems[second.tableName] = {
    Keys: _.range(150).map(function(i) {
      return { id: i.toString() };
    })
  };

  var found = dyno.batchGetItemRequests(params);
  assert.equal(found.length, 3, 'split 300 keys into three requests');
  found.sendAll(function(err, results, unprocessed) {
    assert.ifError(err, 'requests were sent successfully');
    assert.ifError(unprocessed, 'no unprocessed items reported');

    results = results.reduce(function(results, result) {
      if (result.Responses[dynamodb.tableName]) results = results.concat(result.Responses[dynamodb.tableName]);
      if (result.Responses[second.tableName]) results = results.concat(result.Responses[second.tableName]);
      return results;
    }, []);

    assert.equal(results.length, 300, 'all responses were recieved');
    assert.end();
  });
});

second.empty();

dynamodb.test('[requests] batchGet sendAll: no errors, unprocessed items present', function(assert) {
  var original = AWS.Request.prototype.send;

  AWS.Request.prototype.send = function() {
    var params = this.params.RequestItems[dynamodb.tableName].Keys;
    var data = { Responses: {} };
    data.Responses[dynamodb.tableName] = [];

    params.forEach(function(key) {
      if (key.id === '143') {
        data.UnprocessedKeys = {};
        data.UnprocessedKeys[dynamodb.tableName] = { Keys: [key] };
      }

      else data.Responses[dynamodb.tableName].push({
        Item: fixtures[key.id]
      });
    });

    this.removeListener('extractError', AWS.EventListeners.Core.EXTRACT_ERROR);
    this.on('extractError', function(response) { response.error = null; });

    this.removeListener('extractData', AWS.EventListeners.Core.EXTRACT_DATA);
    this.on('extractData', function(response) { response.data = data; });

    this.removeListener('send', AWS.EventListeners.Core.SEND);
    this.on('send', function(response) {
      response.httpResponse.body = '{"mocked":"response"}';
      response.httpResponse.statusCode = 200;
    });

    this.runTo();
    return this.response;
  };

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

  var requests = dyno.batchGetItemRequests(params);
  requests.sendAll(function(err, responses, unprocessed) {
    assert.ifError(err, 'success');
    assert.equal(responses.length, requests.length, 'when present, responses array has as many entries as there were requests');
    assert.equal(unprocessed.length, requests.length, 'when present, unprocessed array has as many entries as there were requests');

    var successCount = responses[0].Responses[dynamodb.tableName].length + responses[1].Responses[dynamodb.tableName].length;
    assert.equal(successCount, 149, '149 items requested successfully');
    assert.equal(unprocessed[0], null, 'first request contained no unprocessed items');

    var expected = { RequestItems: {} };
    expected.RequestItems[dynamodb.tableName] = { Keys: [{ id: '143' }] };
    assert.deepEqual(unprocessed[1].params, expected, 'unprocessed request for expected params');

    assert.equal(typeof unprocessed.sendAll, 'function', 'unprocessed response has bound .sendAll');

    AWS.Request.prototype.send = original;
    assert.end();
  });
});

dynamodb.test('[requests] batchGet sendAll: with errors, unprocessed items present', function(assert) {
  var original = AWS.Request.prototype.send;

  AWS.Request.prototype.send = function() {
    var params = this.params.RequestItems[dynamodb.tableName].Keys;
    var data = { Responses: {} };
    data.Responses[dynamodb.tableName] = [];
    var error;

    params.forEach(function(key) {
      if (key.id === '2') {
        error = new Error('omg! mock error!');
        error.statusCode = 404;
      }

      else if (key.id === '143') {
        data.UnprocessedKeys = {};
        data.UnprocessedKeys[dynamodb.tableName] = { Keys: [key] };
      }

      else data.Responses[dynamodb.tableName].push({
        Item: fixtures[key.id]
      });
    });

    this.removeListener('extractError', AWS.EventListeners.Core.EXTRACT_ERROR);
    this.on('extractError', function(response) { response.error = error || null; });

    this.removeListener('extractData', AWS.EventListeners.Core.EXTRACT_DATA);
    this.on('extractData', function(response) { response.data = data; });

    this.removeListener('send', AWS.EventListeners.Core.SEND);
    this.on('send', function(response) {
      response.httpResponse.body = '{"mocked":"response"}';
      response.httpResponse.statusCode = error ? 404 : 200;
    });

    this.runTo();
    return this.response;
  };

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

  var requests = dyno.batchGetItemRequests(params);
  requests.sendAll(function(err, responses, unprocessed) {
    assert.equal(err.length, requests.length, 'when present, error array has as many entries as there were requests');
    assert.equal(responses.length, requests.length, 'when present, responses array has as many entries as there were requests');
    assert.equal(unprocessed.length, requests.length, 'when present, unprocessed array has as many entries as there were requests');

    assert.equal(err[0].message, 'omg! mock error!', 'first request errored');
    assert.equal(responses[0], null, 'response set to null when error occurred');
    assert.equal(unprocessed[0], null, 'first request contained no unprocessed items');

    var expected = { RequestItems: {} };
    expected.RequestItems[dynamodb.tableName] = { Keys: [{ id: '143' }] };

    assert.equal(err[1], null, 'no error on second request');
    assert.equal(responses[1].Responses[dynamodb.tableName].length, 49, '49 successful requests');
    assert.deepEqual(unprocessed[1].params, expected, 'unprocessed request for expected params');
    assert.equal(typeof unprocessed.sendAll, 'function', 'unprocessed response has bound .sendAll');

    AWS.Request.prototype.send = original;
    assert.end();
  });
});

dynamodb.test('[requests] batchWrite sendAll: no errors, unprocessed items present', function(assert) {
  var original = AWS.Request.prototype.send;

  AWS.Request.prototype.send = function() {
    var params = this.params.RequestItems[dynamodb.tableName];
    var data = { Responses: {} };
    data.Responses[dynamodb.tableName] = [];

    params.forEach(function(req) {
      if (req.PutRequest.Item.id === '143') {
        data.UnprocessedItems = {};
        data.UnprocessedItems[dynamodb.tableName] = [{ PutRequest: { Item: fixtures['143'] } }];
      }

      else data.Responses[dynamodb.tableName].push({});
    });

    this.removeListener('extractError', AWS.EventListeners.Core.EXTRACT_ERROR);
    this.on('extractError', function(response) { response.error = null; });

    this.removeListener('extractData', AWS.EventListeners.Core.EXTRACT_DATA);
    this.on('extractData', function(response) { response.data = data; });

    this.removeListener('send', AWS.EventListeners.Core.SEND);
    this.on('send', function(response) {
      response.httpResponse.body = '{"mocked":"response"}';
      response.httpResponse.statusCode = 200;
    });

    this.runTo();
    return this.response;
  };

  var dyno = Dyno({
    table: dynamodb.tableName,
    region: 'local',
    endpoint: 'http://localhost:4567'
  });

  var params = { RequestItems: {} };
  params.RequestItems[dynamodb.tableName] = fixtures.map(function(item) {
    return { PutRequest: { Item: item } };
  });

  var requests = dyno.batchWriteItemRequests(params);
  requests.sendAll(function(err, responses, unprocessed) {
    assert.ifError(err, 'success');
    assert.equal(responses.length, requests.length, 'when present, responses array has as many entries as there were requests');
    assert.equal(unprocessed.length, requests.length, 'when present, unprocessed array has as many entries as there were requests');

    var successCount =
      responses[0].Responses[dynamodb.tableName].length +
      responses[1].Responses[dynamodb.tableName].length +
      responses[2].Responses[dynamodb.tableName].length +
      responses[3].Responses[dynamodb.tableName].length +
      responses[4].Responses[dynamodb.tableName].length +
      responses[5].Responses[dynamodb.tableName].length;

    assert.equal(successCount, 149, '149 items requested successfully');
    assert.equal(unprocessed[0], null, 'first request contained no unprocessed items');
    assert.equal(unprocessed[1], null, 'second request contained no unprocessed items');
    assert.equal(unprocessed[2], null, 'third request contained no unprocessed items');
    assert.equal(unprocessed[3], null, 'fourth request contained no unprocessed items');
    assert.equal(unprocessed[4], null, 'fifth request contained no unprocessed items');

    var expected = { RequestItems: {} };
    expected.RequestItems[dynamodb.tableName] = [{ PutRequest: { Item: fixtures['143'] } }];
    assert.deepEqual(unprocessed[5].params, expected, 'unprocessed request for expected params');

    assert.equal(typeof unprocessed.sendAll, 'function', 'unprocessed response has bound .sendAll');

    AWS.Request.prototype.send = original;
    assert.end();
  });
});

dynamodb.test('[requests] batchWrite sendAll: with errors, Responses not present', function(assert) {
  var original = AWS.Request.prototype.send;

  AWS.Request.prototype.send = function() {
    var data = { };
    var error = new Error('omg! mock error!');

    this.removeListener('extractError', AWS.EventListeners.Core.EXTRACT_ERROR);
    this.on('extractError', function(response) { response.error = error; });

    this.removeListener('extractData', AWS.EventListeners.Core.EXTRACT_DATA);
    this.on('extractData', function(response) { response.data = data; });

    this.removeListener('send', AWS.EventListeners.Core.SEND);
    this.on('send', function(response) {
      response.httpResponse.body = '{"mocked":"response"}';
      response.httpResponse.statusCode = 400;
    });

    this.runTo();
    return this.response;
  };

  var dyno = Dyno({
    table: dynamodb.tableName,
    region: 'local',
    endpoint: 'http://localhost:4567'
  });

  var params = { RequestItems: {} };
  params.RequestItems[dynamodb.tableName] = fixtures.map(function(item) {
    return { PutRequest: { Item: item } };
  });

  var requests = dyno.batchWriteItemRequests(params);
  requests.sendAll(function(err, responses, unprocessed) {
    assert.equal(err.length, 6, 'has a lot of error message');
    assert.equal(err[0].message, 'omg! mock error!', 'with the expected message');
    assert.equal(responses.filter(function(v) { return v; }).length, 0, 'no none null reponses');
    assert.notOk(unprocessed, 'and nothing left to do');

    AWS.Request.prototype.send = original;
    assert.end();
  });
});

dynamodb.test('[requests] batchWrite sendAll: with errors, unprocessed items present', function(assert) {
  var original = AWS.Request.prototype.send;

  AWS.Request.prototype.send = function() {
    var params = this.params.RequestItems[dynamodb.tableName];
    var data = { Responses: {} };
    data.Responses[dynamodb.tableName] = [];
    var error;

    params.forEach(function(req) {
      if (req.PutRequest.Item.id === '2') {
        error = new Error('omg! mock error!');
        error.statusCode = 404;
      }

      else if (req.PutRequest.Item.id === '143') {
        data.UnprocessedItems = {};
        data.UnprocessedItems[dynamodb.tableName] = [{ PutRequest: { Item: fixtures['143'] } }];
      }

      else data.Responses[dynamodb.tableName].push({});
    });

    this.removeListener('extractError', AWS.EventListeners.Core.EXTRACT_ERROR);
    this.on('extractError', function(response) { response.error = error || null; });

    this.removeListener('extractData', AWS.EventListeners.Core.EXTRACT_DATA);
    this.on('extractData', function(response) { response.data = data; });

    this.removeListener('send', AWS.EventListeners.Core.SEND);
    this.on('send', function(response) {
      response.httpResponse.body = '{"mocked":"response"}';
      response.httpResponse.statusCode = error ? 404 : 200;
    });

    this.runTo();
    return this.response;
  };

  var dyno = Dyno({
    table: dynamodb.tableName,
    region: 'local',
    endpoint: 'http://localhost:4567'
  });

  var params = { RequestItems: {} };
  params.RequestItems[dynamodb.tableName] = fixtures.map(function(item) {
    return { PutRequest: { Item: item } };
  });

  var requests = dyno.batchWriteItemRequests(params);
  requests.sendAll(function(err, responses, unprocessed) {
    assert.equal(err.length, requests.length, 'when present, error array has as many entries as there were requests');
    assert.equal(responses.length, requests.length, 'when present, responses array has as many entries as there were requests');
    assert.equal(unprocessed.length, requests.length, 'when present, unprocessed array has as many entries as there were requests');

    assert.equal(err[0].message, 'omg! mock error!', 'first response errored');
    assert.equal(responses[0], null, 'responses set to null when error occurred');
    assert.equal(unprocessed[0], null, 'no unprocessed results');

    assert.equal(err[1], null, 'second response did not error');
    assert.equal(responses[1].Responses[dynamodb.tableName].length, 25, '25 successful responses');
    assert.equal(unprocessed[1], null, 'no unprocessed results');

    assert.equal(err[2], null, 'third response did not error');
    assert.equal(responses[2].Responses[dynamodb.tableName].length, 25, '25 successful responses');
    assert.equal(unprocessed[2], null, 'no unprocessed results');

    assert.equal(err[3], null, 'fourth response did not error');
    assert.equal(responses[3].Responses[dynamodb.tableName].length, 25, '25 successful responses');
    assert.equal(unprocessed[3], null, 'no unprocessed results');

    assert.equal(err[4], null, 'fifth response did not error');
    assert.equal(responses[4].Responses[dynamodb.tableName].length, 25, '25 successful responses');
    assert.equal(unprocessed[4], null, 'no unprocessed results');

    assert.equal(err[5], null, 'sixth response did not error');
    assert.equal(responses[5].Responses[dynamodb.tableName].length, 24, '24 successful responses');

    var expected = { RequestItems: {} };
    expected.RequestItems[dynamodb.tableName] = [{ PutRequest: { Item: fixtures['143'] } }];
    assert.deepEqual(unprocessed[5].params, expected, 'unprocessed request for expected params');

    assert.equal(typeof unprocessed.sendAll, 'function', 'unprocessed response has bound .sendAll');

    unprocessed.sendAll(function(err) {
      assert.ifError(err, 'successful .sendAll on unprocessed requestSet');
      AWS.Request.prototype.send = original;
      assert.end();
    });
  });
});

dynamodb.test('[requests] batchWriteAll sendAll: no errors, no unprocessed items', function(assert) {
  var dyno = Dyno({
    table: dynamodb.tableName,
    region: 'local',
    endpoint: 'http://localhost:4567'
  });

  var params = { RequestItems: {} };
  params.RequestItems[dynamodb.tableName] = fixtures.map(function(item) {
    return { PutRequest: { Item: item } };
  });

  var requests = dyno.batchWriteAll(params);
  requests.sendAll(function(err) {
    if (err) return assert.end(err);

    dynamodb.dyno.scan(function(err, data) {
      if (err) return assert.end(err);

      assert.equal(data.Items.length, fixtures.length, 'expected data');
      assert.end();
    });
  });
});

test('[requests] batchWriteAll sendAll: with errors, unprocessed items present', function(assert) {
  var original = AWS.Request.prototype.send;
  var once = true;

  AWS.Request.prototype.send = function() {
    var params = this.params.RequestItems[dynamodb.tableName];
    var data = {};

    data.ConsumedCapacity = {
      TableName: dynamodb.tableName,
      CapacityUnits: 10
    };
    var error;

    params.forEach(function(req) {
      if (req.PutRequest.Item.id === '2') {
        error = new Error('omg! mock error!');
        error.statusCode = 404;
      }

      else if (once && req.PutRequest.Item.id === '143') {
        assert.pass('one unprocessed item');
        once = false;
        data.UnprocessedItems = {};
        data.UnprocessedItems[dynamodb.tableName] = [{ PutRequest: { Item: fixtures['143'] } }];
        data.ConsumedCapacity.CapacityUnits = 0;
      }
    });

    this.removeListener('extractError', AWS.EventListeners.Core.EXTRACT_ERROR);
    this.on('extractError', function(response) { response.error = error || null; });

    this.removeListener('extractData', AWS.EventListeners.Core.EXTRACT_DATA);
    this.on('extractData', function(response) { response.data = data; });

    this.removeListener('send', AWS.EventListeners.Core.SEND);
    this.on('send', function(response) {
      response.httpResponse.body = '{"mocked":"response"}';
      response.httpResponse.statusCode = error ? 404 : 200;
    });

    this.runTo();
    return this.response;
  };

  var dyno = Dyno({
    table: dynamodb.tableName,
    region: 'local',
    endpoint: 'http://localhost:4567'
  });

  var params = { RequestItems: {}, ReturnConsumedCapacity: 'TOTAL' };
  params.RequestItems[dynamodb.tableName] = fixtures.map(function(item) {
    return { PutRequest: { Item: item } };
  });

  var requests = dyno.batchWriteAll(params);
  requests.sendAll(function(err, data) {

    assert.equal(err.message, 'omg! mock error!', 'single error was reported from a failed request');

    assert.deepEqual(data.ConsumedCapacity, {
      TableName: dynamodb.tableName,
      CapacityUnits: 50
    }, 'aggregated consumed capacity');

    AWS.Request.prototype.send = original;
    assert.end();
  });
});

dynamodb.test('[requests] batchGetAll sendAll: no errors, no unprocessed items', function(assert) {
  var dyno = Dyno({
    table: dynamodb.tableName,
    region: 'local',
    endpoint: 'http://localhost:4567'
  });

  var keys = _.range(150).map(function(i) {
    return { id: i.toString() };
  });

  var fixtureParams = { RequestItems: {} };
  fixtureParams.RequestItems[dynamodb.tableName] = keys.map(function(key) {
    return { PutRequest: { Item: key } };
  });
  dyno.batchWriteAll(fixtureParams).sendAll(10, function(err) {
    if (err) return assert.end(err);

    var params = { RequestItems: {} };
    params.RequestItems[dynamodb.tableName] = { Keys: keys };

    var requests = dyno.batchGetAll(params);
    requests.sendAll(function(err, data) {
      if (err) return assert.end(err);

      assert.equal(Object.keys(data.Responses).reduce(function(total, table) {
        total += data.Responses[table].length;
        return total;
      }, 0), 150, '150 successful responses');

      assert.end();
    });
  });
});

dynamodb.test('[requests] batchGetAll sendAll: with errors, unprocessed items present', function(assert) {
  var original = AWS.Request.prototype.send;
  var once = true;

  AWS.Request.prototype.send = function() {
    var params = this.params.RequestItems[dynamodb.tableName].Keys;
    var data = { Responses: {} };
    data.Responses[dynamodb.tableName] = [];
    if (this.params.ReturnConsumedCapacity) data.ConsumedCapacity = {
      TableName: dynamodb.tableName,
      CapacityUnits: 10
    };
    var error;

    params.forEach(function(key) {
      if (key.id === '2') {
        error = new Error('omg! mock error!');
        error.statusCode = 404;
      }

      else if (once && key.id === '143') {
        assert.pass('one unprocessed item');
        once = false;
        data.UnprocessedKeys = {};
        data.UnprocessedKeys[dynamodb.tableName] = { Keys: [key] };
      }

      else data.Responses[dynamodb.tableName].push({
        Item: { id: key.id }
      });
    });

    this.removeListener('extractError', AWS.EventListeners.Core.EXTRACT_ERROR);
    this.on('extractError', function(response) { response.error = error || null; });

    this.removeListener('extractData', AWS.EventListeners.Core.EXTRACT_DATA);
    this.on('extractData', function(response) { response.data = data; });

    this.removeListener('send', AWS.EventListeners.Core.SEND);
    this.on('send', function(response) {
      response.httpResponse.body = '{"mocked":"response"}';
      response.httpResponse.statusCode = error ? 404 : 200;
    });

    this.runTo();
    return this.response;
  };

  var dyno = Dyno({
    table: dynamodb.tableName,
    region: 'local',
    endpoint: 'http://localhost:4567'
  });

  var params = { RequestItems: {}, ReturnConsumedCapacity: 'TOTAL' };
  params.RequestItems[dynamodb.tableName] = {
    Keys: _.range(150).map(function(i) {
      return { id: i.toString() };
    })
  };

  var requests = dyno.batchGetAll(params);
  requests.sendAll(function(err, data) {
    assert.equal(err.message, 'omg! mock error!', 'single error was reported from a failed request');

    assert.equal(Object.keys(data.Responses).reduce(function(total, table) {
      return total + data.Responses[table].length;
    }, 0), 50, '50 successful responses (100 lost in error request)');
    assert.deepEqual(data.ConsumedCapacity, {
      TableName: dynamodb.tableName,
      CapacityUnits: 20
    }, 'aggregated consumed capacity from 2 requests');

    AWS.Request.prototype.send = original;
    assert.end();
  });
});

dynamodb.test('[requests] batchGetAll sendAll: everything is unprocessed. timeout', function(assert) {
  var original = AWS.Request.prototype.send;

  AWS.Request.prototype.send = function() {
    var params = this.params.RequestItems[dynamodb.tableName].Keys;
    var data = { Responses: {}, UnprocessedKeys: {} };
    data.UnprocessedKeys[dynamodb.tableName] = { Keys: params };

    var capacity = this.params.ReturnConsumedCapacity;
    if (capacity) data.ConsumedCapacity = {
      TableName: dynamodb.tableName,
      CapacityUnits: 10
    };

    this.removeListener('extractError', AWS.EventListeners.Core.EXTRACT_ERROR);
    this.on('extractError', function(response) { response.error = null; });

    this.removeListener('extractData', AWS.EventListeners.Core.EXTRACT_DATA);
    this.on('extractData', function(response) { response.data = data; });

    this.removeListener('send', AWS.EventListeners.Core.SEND);
    this.on('send', function(response) {
      response.httpResponse.body = '{"mocked":"response"}';
      response.httpResponse.statusCode = 400;
    });

    this.runTo();
    return this.response;
  };

  var dyno = Dyno({
    table: dynamodb.tableName,
    region: 'local',
    endpoint: 'http://localhost:4567'
  });

  var params = { RequestItems: {}, ReturnConsumedCapacity: 'TOTAL' };
  params.RequestItems[dynamodb.tableName] = {
    Keys: _.range(150).map(function(i) {
      return { id: i.toString() };
    })
  };

  var requests = dyno.batchGetAll(params, 3);
  requests.sendAll(function(err, data) {
    assert.ifError(err, 'there is no error here');
    assert.equal(data.Responses[dynamodb.tableName], undefined, 'there are no responses');
    assert.equal(data.UnprocessedKeys[dynamodb.tableName].Keys.length, 150, 'there are unprocessed keys');
    assert.deepEqual(data.ConsumedCapacity, {
      TableName: dynamodb.tableName,
      CapacityUnits: 80
    }, 'aggregated consumed capacity from 2 requests');

    AWS.Request.prototype.send = original;
    assert.end();
  });
});

test('[requests] batchWriteAll sendAll: everything is unprocessed. timeout', function(assert) {
  var original = AWS.Request.prototype.send;

  AWS.Request.prototype.send = function() {
    var params = this.params.RequestItems[dynamodb.tableName];
    var data = { Responses: {} };
    data.Responses[dynamodb.tableName] = [];
    data.UnprocessedItems = {};
    data.UnprocessedItems[dynamodb.tableName] = params.map(function(req) {
      return { PutRequest: { Item: fixtures[req.PutRequest.Item.id] } };
    });

    var capacity = this.params.ReturnConsumedCapacity;
    if (capacity) data.ConsumedCapacity = {
      TableName: dynamodb.tableName,
      CapacityUnits: 10
    };

    this.removeListener('extractError', AWS.EventListeners.Core.EXTRACT_ERROR);
    this.on('extractError', function(response) { response.error = null; });

    this.removeListener('extractData', AWS.EventListeners.Core.EXTRACT_DATA);
    this.on('extractData', function(response) { response.data = data; });

    this.removeListener('send', AWS.EventListeners.Core.SEND);
    this.on('send', function(response) {
      response.httpResponse.body = '{"mocked":"response"}';
      // Question: Should this be 200?
      response.httpResponse.statusCode = 200;
    });

    this.runTo();
    return this.response;
  };

  var dyno = Dyno({
    table: dynamodb.tableName,
    region: 'local',
    endpoint: 'http://localhost:4567'
  });

  var params = { RequestItems: {}, ReturnConsumedCapacity: 'TOTAL' };
  params.RequestItems[dynamodb.tableName] = fixtures.map(function(item) {
    return { PutRequest: { Item: item } };
  });

  var requests = dyno.batchWriteAll(params, 3);
  requests.sendAll(function(err, data) {
    assert.equal(data.Responses[dynamodb.tableName].length, 0, 'there are no responses');
    assert.equal(data.UnprocessedItems[dynamodb.tableName].length, 150, 'all items were left unprocessed');
    assert.deepEqual(data.ConsumedCapacity, {
      TableName: dynamodb.tableName,
      CapacityUnits: 240
    }, 'aggregated consumed capacity');

    AWS.Request.prototype.send = original;
    assert.end();
  });
});

second.delete();
dynamodb.delete();

dynamodb.close();
