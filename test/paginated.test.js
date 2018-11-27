var test = require('tape');
var testTables = require('./test-tables');
var dynamodb = require('@mapbox/dynamodb-test')(test, 'dyno', testTables['idhash-numrange']);
var crypto = require('crypto');
var _ = require('underscore');
var Dyno = require('..');

var fixtures = _.range(2345).map(function(i) {
  return {
    id: 'id',
    num: i,
    data: crypto.randomBytes(5 * 1024)
  };
});

dynamodb.test('[paginated] query, 0 pages', fixtures, function(assert) {
  var dyno = Dyno({
    table: dynamodb.tableName,
    region: 'local',
    endpoint: 'http://localhost:4567'
  });

  dyno.query({
    ExpressionAttributeNames: { '#id': 'id' },
    ExpressionAttributeValues: { ':id': 'id' },
    KeyConditionExpression: '#id = :id',
    Pages: 0
  }, function(err) {
    assert.equal(err.message, 'Pages must be an integer greater than 0', '0 Pages is an invalid parameter value');
    assert.end();
  });
});

dynamodb.test('[paginated] query, negative pages', fixtures, function(assert) {
  var dyno = Dyno({
    table: dynamodb.tableName,
    region: 'local',
    endpoint: 'http://localhost:4567'
  });

  dyno.query({
    ExpressionAttributeNames: { '#id': 'id' },
    ExpressionAttributeValues: { ':id': 'id' },
    KeyConditionExpression: '#id = :id',
    Pages: -56
  }, function(err) {
    assert.equal(err.message, 'Pages must be an integer greater than 0', 'Pages must be greater than 0');
    assert.end();
  });
});

dynamodb.test('[paginated] query, string for pages', fixtures, function(assert) {
  var dyno = Dyno({
    table: dynamodb.tableName,
    region: 'local',
    endpoint: 'http://localhost:4567'
  });

  dyno.query({
    ExpressionAttributeNames: { '#id': 'id' },
    ExpressionAttributeValues: { ':id': 'id' },
    KeyConditionExpression: '#id = :id',
    Pages: 'five'
  }, function(err) {
    assert.equal(err.message, 'Pages must be an integer greater than 0', 'Pages must be a number');
    assert.end();
  });
});

dynamodb.test('[paginated] query, 1 page', fixtures, function(assert) {
  var dyno = Dyno({
    table: dynamodb.tableName,
    region: 'local',
    endpoint: 'http://localhost:4567'
  });

  dyno.query({
    ExpressionAttributeNames: { '#id': 'id' },
    ExpressionAttributeValues: { ':id': 'id' },
    KeyConditionExpression: '#id = :id',
    Pages: 1
  }, function(err, data) {
    if (err) return assert.end(err);
    var items = fixtures.slice(0, 203);
    assert.deepEqual(data, {
      Items: items,
      Count: items.length,
      ScannedCount: items.length,
      LastEvaluatedKey: { id: 'id', num: 202 },
      ConsumedCapacity: undefined
    }, 'one page of results');
    assert.end();
  });
});

dynamodb.test('[paginated] query, undefined Pages', fixtures, function(assert) {
  var dyno = Dyno({
    table: dynamodb.tableName,
    region: 'local',
    endpoint: 'http://localhost:4567'
  });

  dyno.query({
    ExpressionAttributeNames: { '#id': 'id' },
    ExpressionAttributeValues: { ':id': 'id' },
    KeyConditionExpression: '#id = :id'
  }, function(err, data) {
    if (err) return assert.end(err);
    var items = fixtures.slice(0, 203);
    assert.deepEqual(data, {
      Items: items,
      Count: items.length,
      ScannedCount: items.length,
      LastEvaluatedKey: { id: 'id', num: 202 }
    }, 'one page of results');
    assert.end();
  });
});

dynamodb.test('[paginated] query, null Pages', fixtures, function(assert) {
  var dyno = Dyno({
    table: dynamodb.tableName,
    region: 'local',
    endpoint: 'http://localhost:4567'
  });

  dyno.query({
    ExpressionAttributeNames: { '#id': 'id' },
    ExpressionAttributeValues: { ':id': 'id' },
    KeyConditionExpression: '#id = :id',
    Pages: null
  }, function(err, data) {
    if (err) return assert.end(err);
    var items = fixtures.slice(0, 203);
    assert.deepEqual(data, {
      Items: items,
      Count: items.length,
      ScannedCount: items.length,
      LastEvaluatedKey: { id: 'id', num: 202 }
    }, 'one page of results');
    assert.end();
  });
});

dynamodb.test('[paginated] query, 2 pages', fixtures, function(assert) {
  var dyno = Dyno({
    table: dynamodb.tableName,
    region: 'local',
    endpoint: 'http://localhost:4567'
  });

  dyno.query({
    ExpressionAttributeNames: { '#id': 'id' },
    ExpressionAttributeValues: { ':id': 'id' },
    KeyConditionExpression: '#id = :id',
    Pages: 2,
    ReturnConsumedCapacity: 'TOTAL'
  }, function(err, data) {
    if (err) return assert.end(err);
    var items = fixtures.slice(0, 406);
    assert.deepEqual(data, {
      Items: items,
      Count: items.length,
      ScannedCount: items.length,
      LastEvaluatedKey: { id: 'id', num: 405 },
      ConsumedCapacity: { CapacityUnits: 255, TableName: dynamodb.tableName }
    }, 'two pages of results');
    assert.end();
  });
});

dynamodb.test('[paginated] query, Infinity pages', fixtures, function(assert) {
  var dyno = Dyno({
    table: dynamodb.tableName,
    region: 'local',
    endpoint: 'http://localhost:4567'
  });

  dyno.query({
    ExpressionAttributeNames: { '#id': 'id' },
    ExpressionAttributeValues: { ':id': 'id' },
    KeyConditionExpression: '#id = :id',
    Pages: Infinity
  }, function(err, data) {
    if (err) return assert.end(err);
    assert.deepEqual(data, {
      Items: fixtures,
      Count: fixtures.length,
      ScannedCount: fixtures.length,
      LastEvaluatedKey: undefined,
      ConsumedCapacity: undefined
    }, 'Infinity pages of results');
    assert.end();
  });
});

dynamodb.test('[paginated] scan, 0 pages', fixtures, function(assert) {
  var dyno = Dyno({
    table: dynamodb.tableName,
    region: 'local',
    endpoint: 'http://localhost:4567'
  });

  dyno.scan({
    ExpressionAttributeNames: { '#id': 'id' },
    ExpressionAttributeValues: { ':id': 'id' },
    KeyConditionExpression: '#id = :id',
    Pages: 0
  }, function(err) {
    assert.equal(err.message, 'Pages must be an integer greater than 0', '0 Pages is an invalid parameter value');
    assert.end();
  });
});

dynamodb.test('[paginated] scan, negative pages', fixtures, function(assert) {
  var dyno = Dyno({
    table: dynamodb.tableName,
    region: 'local',
    endpoint: 'http://localhost:4567'
  });

  dyno.scan({
    ExpressionAttributeNames: { '#id': 'id' },
    ExpressionAttributeValues: { ':id': 'id' },
    KeyConditionExpression: '#id = :id',
    Pages: -56
  }, function(err) {
    assert.equal(err.message, 'Pages must be an integer greater than 0', 'Pages must be greater than 0');
    assert.end();
  });
});

dynamodb.test('[paginated] scan, string for pages', fixtures, function(assert) {
  var dyno = Dyno({
    table: dynamodb.tableName,
    region: 'local',
    endpoint: 'http://localhost:4567'
  });

  dyno.scan({
    ExpressionAttributeNames: { '#id': 'id' },
    ExpressionAttributeValues: { ':id': 'id' },
    KeyConditionExpression: '#id = :id',
    Pages: 'five'
  }, function(err) {
    assert.equal(err.message, 'Pages must be an integer greater than 0', 'Pages must be a number');
    assert.end();
  });
});

dynamodb.test('[paginated] scan, 1 page', fixtures, function(assert) {
  var dyno = Dyno({
    table: dynamodb.tableName,
    region: 'local',
    endpoint: 'http://localhost:4567'
  });

  dyno.scan({
    Pages: 1
  }, function(err, data) {
    if (err) return assert.end(err);
    var items = fixtures.slice(0, 203);
    assert.deepEqual(data, {
      Items: items,
      Count: items.length,
      ScannedCount: items.length,
      LastEvaluatedKey: { id: 'id', num: 202 },
      ConsumedCapacity: undefined
    }, 'one page of results');
    assert.end();
  });
});

dynamodb.test('[paginated] scan, undefined Pages', fixtures, function(assert) {
  var dyno = Dyno({
    table: dynamodb.tableName,
    region: 'local',
    endpoint: 'http://localhost:4567'
  });

  dyno.scan(function(err, data) {
    if (err) return assert.end(err);
    var items = fixtures.slice(0, 203);
    assert.deepEqual(data, {
      Items: items,
      Count: items.length,
      ScannedCount: items.length,
      LastEvaluatedKey: { id: 'id', num: 202 }
    }, 'one page of results');
    assert.end();
  });
});

dynamodb.test('[paginated] scan, null Pages', fixtures, function(assert) {
  var dyno = Dyno({
    table: dynamodb.tableName,
    region: 'local',
    endpoint: 'http://localhost:4567'
  });

  dyno.scan({
    Pages: null
  }, function(err, data) {
    if (err) return assert.end(err);
    var items = fixtures.slice(0, 203);
    assert.deepEqual(data, {
      Items: items,
      Count: items.length,
      ScannedCount: items.length,
      LastEvaluatedKey: { id: 'id', num: 202 }
    }, 'one page of results');
    assert.end();
  });
});

dynamodb.test('[paginated] scan, 2 pages', fixtures, function(assert) {
  var dyno = Dyno({
    table: dynamodb.tableName,
    region: 'local',
    endpoint: 'http://localhost:4567'
  });

  dyno.scan({
    Pages: 2,
    ReturnConsumedCapacity: 'TOTAL'
  }, function(err, data) {
    if (err) return assert.end(err);
    var items = fixtures.slice(0, 406);
    assert.deepEqual(data, {
      Items: items,
      Count: items.length,
      ScannedCount: items.length,
      LastEvaluatedKey: { id: 'id', num: 405 },
      ConsumedCapacity: { CapacityUnits: 255, TableName: dynamodb.tableName }
    }, 'two pages of results');
    assert.end();
  });
});

dynamodb.test('[paginated] scan, Infinity pages', fixtures, function(assert) {
  var dyno = Dyno({
    table: dynamodb.tableName,
    region: 'local',
    endpoint: 'http://localhost:4567'
  });

  dyno.scan({
    Pages: Infinity
  }, function(err, data) {
    if (err) return assert.end(err);
    assert.deepEqual(data, {
      Items: fixtures,
      Count: fixtures.length,
      ScannedCount: fixtures.length,
      LastEvaluatedKey: undefined,
      ConsumedCapacity: undefined
    }, 'Infinity pages of results');
    assert.end();
  });
});

dynamodb.close();
