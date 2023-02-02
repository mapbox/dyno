/* eslint-env es6 */
const test = require('tape');
var _ = require('underscore');
var crypto = require('crypto');

const reduceCapacity = require('../lib/util').reduceCapacity;
const requestHandler = require('../lib/util').requestHandler;
// const wrapClient = require('../lib/util').wrapClient;
// const wrapDocClient = require('../lib/util').wrapDocClient;
const sinon = require('sinon');
var Dyno = require('..');
var testTables = require('./test-tables');
var dynamodb = require('@mapbox/dynamodb-test')(test, 'dyno', testTables.idhash);

function randomItems(num) {
  return _.range(num).map(function(i) {
    return {
      id: i.toString(),
      range: i,
      data: crypto.randomBytes(36)
    };
  });
}

test('[reduceCapacity] parses new data format correctly', function (assert) {
  const src = [{
    TableName: 'db-staging',
    CapacityUnits: 8,
    ReadCapacityUnits: 3,
    WriteCapacityUnits: 1,
    Table: { CapacityUnits: 4, ReadCapacityUnits: 3, WriteCapacityUnits: 1 },
    GlobalSecondaryIndexes: { 'id-index': { CapacityUnits: 4 } }
  }];
  const dst = {};

  reduceCapacity(dst, src);

  assert.equal(dst.CapacityUnits, 8);
  assert.equal(dst.Table.CapacityUnits, 4);
  assert.equal(dst.GlobalSecondaryIndexes['id-index'].CapacityUnits, 4);
  assert.end();
});

test('[reduceCapacity] parses old data format correctly', function (assert) {
  const src = {
    TableName: 'db-staging',
    CapacityUnits: 8,
    Table: { CapacityUnits: 4 },
    GlobalSecondaryIndexes: { CapacityUnits: 4 }
  };
  const dst = {};

  reduceCapacity(dst, src);

  assert.equal(dst.CapacityUnits, 8);
  assert.equal(dst.Table.CapacityUnits, 4);
  assert.equal(dst.GlobalSecondaryIndexes.CapacityUnits, 4);
  assert.end();
});

test('[reduceCapacity] merges old data format correctly', function (assert) {
  const src = [{
    TableName: 'db-staging',
    CapacityUnits: 8,
    Table: { CapacityUnits: 4 },
    GlobalSecondaryIndexes: { 'id-index': { CapacityUnits: 4 } }
  }];
  const dst = {
    CapacityUnits: 2,
    Table: { CapacityUnits: 1 },
    GlobalSecondaryIndexes: { 'di-index': { CapacityUnits: 4 } }
  };

  reduceCapacity(dst, src);

  assert.equal(dst.CapacityUnits, 10);
  assert.equal(dst.Table.CapacityUnits, 5);
  assert.equal(dst.GlobalSecondaryIndexes['id-index'].CapacityUnits, 4);
  assert.equal(dst.GlobalSecondaryIndexes['di-index'].CapacityUnits, 4);
  assert.end();
});


test('[reduceCapacity] does not crash if dst is empty', function (assert) {
  reduceCapacity(null, []);
  assert.end();
});

test('[requestHandler] returns original method if no costLogger', function (assert) {
  const func = () => {};
  assert.strictEqual(requestHandler(null, func), func);
  assert.end();
});

test('[requestHandler] throws error if capacity type is incorrect', function (assert) {
  assert.throws(() => {
    requestHandler({}, () => {}, 'foo');
  }, new Error('Invalid capacity type'));
  assert.end();
});

test ('[requestHandler] ReturnConsumedCapacity param is set as INDEXES', function (assert) {
  let newParams;
  const nativeMethod = function (params, callback) {
    newParams = params;
    callback(null, {
      ConsumedCapacity: 100
    });
  };
  const wrappedMethod = requestHandler(function(){}, nativeMethod, 'Write');
  wrappedMethod({}, function(){});
  assert.equal(newParams.ReturnConsumedCapacity, 'INDEXES', 'ReturnConsumedCapacity is INDEXES');
  assert.end();
});


test ('[requestHandler] do not call costLogger if no consumedCapacity', function (assert) {
  const costLogger = sinon.stub();
  const nativeMethod = function (params, callback) {
    callback(null, {
      ConsumedCapacity: 0
    });
  };
  const wrappedMethod = requestHandler(costLogger, nativeMethod, 'Write');
  wrappedMethod({}, function(){});
  assert.ok(costLogger.notCalled, 'costLogger is not called');
  assert.end();
});

dynamodb.start();

dynamodb.test('[costLogger] client batchGet', function(assert) {
  const costLoggerStub = sinon.stub();
  var dyno = Dyno({
    table: dynamodb.tableName,
    region: 'local',
    endpoint: 'http://localhost:4567',
    costLogger: costLoggerStub
  });
  var params = { RequestItems: {} };
  params.RequestItems[dynamodb.tableName] = {
    Keys: _.range(10).map(function(i) {
      return { id: i.toString() };
    })
  };
  dyno.batchGetItem(params, function(err) {
    assert.notOk(err, 'no error');
    console.log(costLoggerStub.getCall(0).args);
    assert.ok(costLoggerStub.calledWith({ ConsumedCapacity: { ReadCapacityUnits: 5 } }), 'costLoggerStub is called');
    assert.end();
  });
});

dynamodb.test('[costLogger] batchWrite', function(assert) {
  const costLoggerStub = sinon.stub();
  var dyno = Dyno({
    table: dynamodb.tableName,
    region: 'local',
    endpoint: 'http://localhost:4567',
    costLogger: costLoggerStub
  });
  var records = randomItems(10);
  var params = { RequestItems: {} };
  params.RequestItems[dynamodb.tableName] = records.map(function(item) {
    return {
      PutRequest: { Item: item }
    };
  });
  dyno.batchWriteItem(params, function(err) {
    assert.notOk(err, 'no error');
    console.log(costLoggerStub.getCall(0).args);
    assert.ok(costLoggerStub.calledWith({ ConsumedCapacity: { WriteCapacityUnits: 10 } }), 'costLoggerStub is called');
    assert.end();
  });
});

dynamodb.test('[costLogger] get', function(assert) {
  const costLoggerStub = sinon.stub();
  var dyno = Dyno({
    table: dynamodb.tableName,
    region: 'local',
    endpoint: 'http://localhost:4567',
    costLogger: costLoggerStub
  });
  const params = {
    TableName: dynamodb.tableName,
    Key: { id: '1' }
  };
  dyno.getItem(params, function(err) {
    assert.notOk(err, 'no error');
    console.log(costLoggerStub.getCall(0).args);
    assert.ok(costLoggerStub.calledWith({ ConsumedCapacity: { ReadCapacityUnits: 0.5 } }), 'costLoggerStub is called');
    assert.end();
  });
});

dynamodb.test('[costLogger] delete', function(assert) {
  const costLoggerStub = sinon.stub();
  var dyno = Dyno({
    table: dynamodb.tableName,
    region: 'local',
    endpoint: 'http://localhost:4567',
    costLogger: costLoggerStub
  });
  const params = {
    TableName: dynamodb.tableName,
    Key: { id: '1' }
  };
  dyno.deleteItem(params, function(err) {
    assert.notOk(err, 'no error');
    console.log(costLoggerStub.getCall(0).args);
    assert.ok(costLoggerStub.calledWith({ ConsumedCapacity: { WriteCapacityUnits: 1 } }), 'costLoggerStub is called');
    assert.end();
  });
});

dynamodb.test('[costLogger] put', function(assert) {
  const costLoggerStub = sinon.stub();
  var dyno = Dyno({
    table: dynamodb.tableName,
    region: 'local',
    endpoint: 'http://localhost:4567',
    costLogger: costLoggerStub
  });
  const params = {
    TableName: dynamodb.tableName,
    Item: randomItems(1)[0]
  };
  dyno.putItem(params, function(err) {
    assert.notOk(err, 'no error');
    console.log(costLoggerStub.getCall(0).args);
    assert.ok(costLoggerStub.calledWith({ ConsumedCapacity: { WriteCapacityUnits: 1 } }), 'costLoggerStub is called');
    assert.end();
  });
});

dynamodb.test('[costLogger] update', function(assert) {
  const costLoggerStub = sinon.stub();
  var dyno = Dyno({
    table: dynamodb.tableName,
    region: 'local',
    endpoint: 'http://localhost:4567',
    costLogger: costLoggerStub
  });
  const params = {
    TableName: dynamodb.tableName,
    Key: {
      id: '0'
    }
  };
  dyno.updateItem(params, function(err) {
    assert.notOk(err, 'no error');
    console.log(costLoggerStub.getCall(0).args);
    assert.ok(costLoggerStub.calledWith({ ConsumedCapacity: { WriteCapacityUnits: 1 } }), 'costLoggerStub is called');
    assert.end();
  });
});

dynamodb.test('[costLogger] scan', function(assert) {
  const costLoggerStub = sinon.stub();
  var dyno = Dyno({
    table: dynamodb.tableName,
    region: 'local',
    endpoint: 'http://localhost:4567',
    costLogger: costLoggerStub
  });
  const params = {
    TableName: dynamodb.tableName,
    Pages: 1
  };
  dyno.putItem({
    TableName: dynamodb.tableName,
    Item: randomItems(1)[0]
  }, function() {
    dyno.scan(params, function(err) {
      assert.notOk(err, 'no error');
      assert.deepEqual(costLoggerStub.getCall(1).args[0], { ConsumedCapacity: { ReadCapacityUnits: 0.5 } }, 'costLoggerStub is called');
      assert.end();
    });
  });
});

dynamodb.test('[costLogger] query', function(assert) {
  const costLoggerStub = sinon.stub();
  var dyno = Dyno({
    table: dynamodb.tableName,
    region: 'local',
    endpoint: 'http://localhost:4567',
    costLogger: costLoggerStub
  });
  const item = randomItems(1)[0];
  const params = {
    TableName: dynamodb.tableName,
    ExpressionAttributeNames: { '#id': 'id' },
    ExpressionAttributeValues: { ':id': item.id },
    KeyConditionExpression: '#id = :id',
    Pages: 1 
  };
  dyno.putItem({
    TableName: dynamodb.tableName,
    Item: item
  }, function() {
    dyno.query(params, function(err) {
      assert.notOk(err, 'no error');
      assert.deepEqual(costLoggerStub.getCall(1).args[0], { ConsumedCapacity: { ReadCapacityUnits: 0.5 } }, 'costLoggerStub is called');
      assert.end();
    });
  });
});
dynamodb.delete();
dynamodb.close();
