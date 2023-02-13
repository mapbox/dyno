/* eslint-disable no-unused-vars */
/* eslint-env es6 */
const test = require('tape');
var _ = require('underscore');
var crypto = require('crypto');

const reduceCapacity = require('../lib/util').reduceCapacity;
const requestHandler = require('../lib/util').requestHandler;
const castIndexesCapacity = require('../lib/util').castIndexesCapacity;
const sinon = require('sinon');
var Dyno = require('..');
var testTables = require('./test-tables');
var dynamodb = require('@mapbox/dynamodb-test')(test, 'dyno', testTables.idhash);
var fixtures = _.range(500).map(function(i) {
  return {
    id: i.toString(),
    range: i,
    data: crypto.randomBytes(5 * 1024)
  };
});

function randomItems(num, bites) {
  return _.range(num).map(function(i) {
    return {
      id: i.toString(),
      range: i,
      data: crypto.randomBytes(bites || 36)
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

test ('[requestHandler] return correct Time - non Stream', function (assert) {
  const costLogger = sinon.stub();
  const timerStub = sinon.stub(Date, 'now');
  timerStub.onFirstCall().returns(1000).onSecondCall().returns(1100);

  const nativeMethod = function (params, callback) {
    callback(null, {
      ConsumedCapacity: 100
    });
  };
  const wrappedMethod = requestHandler(costLogger, nativeMethod, 'Write');
  wrappedMethod({}, function(){});
  timerStub.restore();
  assert.equal(costLogger.getCall(0).args[0].ConsumedCapacity.Time, 100, 'Consumed Time is 100');
  assert.end();
});

test ('[requestHandler] return correct Time - Stream', function (assert) {
  const costLogger = sinon.stub();
  const timerStub = sinon.stub(Date, 'now');
  timerStub
    .onFirstCall()
    .returns(1000)
    .onSecondCall()
    .returns(1100)
    .onThirdCall()
    .returns(1300);

  const nativeMethod = function () {
    return {
      send: function (callback) {
        callback(null, { ConsumedCapacity: 100 });
      }
    };
  };
  const wrappedMethod = requestHandler(costLogger, nativeMethod, 'Write');
  wrappedMethod({}).send();
  assert.equal(costLogger.getCall(0).args[0].ConsumedCapacity.Time, 200, 'Consumed Time is 200');
  assert.end();
  timerStub.restore();
  
});

test('[castIndexesCapacity] cast indexes correctly', function(assert) {
  const indexes = {
    'create-index': {
      CapacityUnits: 100
    },
    'delete-index': {
      WriteCapacityUnits: 200
    }
  };
  assert.deepEqual(castIndexesCapacity(indexes, 'WriteCapacityUnits'), {
    'create-index': {
      WriteCapacityUnits: 100
    },
    'delete-index': {
      WriteCapacityUnits: 200
    }
  }, 'Casted correctly');
  assert.end();
});

dynamodb.start();

dynamodb.test('[costLogger] client batchGet', fixtures, function(assert) {
  const costLoggerStub = sinon.stub();
  const timeStub = sinon.stub(Date, 'now').returns(10000);
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
    assert.ok(costLoggerStub.calledWith({ 
      ConsumedCapacity: { ReadCapacityUnits: 10, GlobalSecondaryIndexes: null, LocalSecondaryIndexes: null, Time: 0 } 
    }), 'costLoggerStub is called');
    assert.end();
    timeStub.restore();
  });
});

dynamodb.test('[costLogger] batchWrite', function(assert) {
  const costLoggerStub = sinon.stub();
  const timeStub = sinon.stub(Date, 'now').returns(10000);
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
    assert.ok(costLoggerStub.calledWith({ 
      ConsumedCapacity: { WriteCapacityUnits: 10, GlobalSecondaryIndexes: null, LocalSecondaryIndexes: null, Time: 0 } 
    }), 'costLoggerStub is called');
    assert.end();
    timeStub.restore();
  });
});

dynamodb.test('[costLogger] get', fixtures, function(assert) {
  const costLoggerStub = sinon.stub();
  const timeStub = sinon.stub(Date, 'now').returns(10000);
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
    assert.ok(costLoggerStub.calledWith({ 
      ConsumedCapacity: { ReadCapacityUnits: 1, GlobalSecondaryIndexes: null, LocalSecondaryIndexes: null, Time: 0 } 
    }), 'costLoggerStub is called');
    assert.end();
    timeStub.restore();
  });
});

dynamodb.test('[costLogger] delete', fixtures, function(assert) {
  const costLoggerStub = sinon.stub();
  const timeStub = sinon.stub(Date, 'now').returns(10000);
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
    assert.ok(costLoggerStub.calledWith({ 
      ConsumedCapacity: { WriteCapacityUnits: 6, GlobalSecondaryIndexes: null, LocalSecondaryIndexes: null, Time: 0 } 
    }), 'costLoggerStub is called');
    assert.end();
    timeStub.restore();
  });
});

dynamodb.test('[costLogger] put', fixtures, function(assert) {
  const costLoggerStub = sinon.stub();
  const timeStub = sinon.stub(Date, 'now').returns(10000);
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
    assert.ok(costLoggerStub.calledWith({ 
      ConsumedCapacity: { WriteCapacityUnits: 6, GlobalSecondaryIndexes: null, LocalSecondaryIndexes: null, Time: 0 } 
    }), 'costLoggerStub is called');
    assert.end();
    timeStub.restore();
  });
});

dynamodb.test('[costLogger] update', fixtures, function(assert) {
  const costLoggerStub = sinon.stub();
  const timeStub = sinon.stub(Date, 'now').returns(10000);
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
    assert.ok(costLoggerStub.calledWith({ 
      ConsumedCapacity: { WriteCapacityUnits: 6, GlobalSecondaryIndexes: null, LocalSecondaryIndexes: null, Time: 0 } 
    }), 'costLoggerStub is called');
    assert.end();
    timeStub.restore();
  });
});

dynamodb.test('[costLogger] scan 1 page', fixtures, function(assert) {
  const costLoggerStub = sinon.stub();
  const timeStub = sinon.stub(Date, 'now').returns(10000);
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
  dyno.scan(params, function(err) {
    assert.notOk(err, 'no error');
    assert.deepEqual(costLoggerStub.getCall(0).args[0], 
      { ConsumedCapacity: { ReadCapacityUnits: 127.5, GlobalSecondaryIndexes: null, LocalSecondaryIndexes: null, Time: 0  } }, 
      'costLoggerStub is called');
    assert.notOk(costLoggerStub.getCall(1), 'only called 1 time'); 
    assert.end();
    timeStub.restore();
  });
});

dynamodb.test('[costLogger] query 1 page', fixtures, function(assert) {
  const costLoggerStub = sinon.stub();
  const timeStub = sinon.stub(Date, 'now').returns(10000);
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
  dyno.query(params, function(err) {
    assert.notOk(err, 'no error');
    assert.deepEqual(costLoggerStub.getCall(0).args[0], 
      { ConsumedCapacity: { ReadCapacityUnits: 1, GlobalSecondaryIndexes: null, LocalSecondaryIndexes: null, Time: 0  } }, 
      'costLoggerStub is called');
    assert.notOk(costLoggerStub.getCall(1), 'only called 1 time'); 
    assert.end();
    timeStub.restore();
  });
});

dynamodb.test('[costLogger] scan', fixtures, function(assert) {
  const costLoggerStub = sinon.stub();
  const timeStub = sinon.stub(Date, 'now').returns(10000);
  var dyno = Dyno({
    table: dynamodb.tableName,
    region: 'local',
    endpoint: 'http://localhost:4567',
    costLogger: costLoggerStub
  });
  const scanParams = {
    TableName: dynamodb.tableName,
    Pages: 5
  };
  dyno.scan(scanParams, function(err) {
    assert.notOk(err, 'no scan error');
    assert.deepEqual(costLoggerStub.getCall(1).args[0], { ConsumedCapacity: { ReadCapacityUnits: 127.5, GlobalSecondaryIndexes: null, LocalSecondaryIndexes: null, Time: 0 } }, 'costLoggerStub is called');
    assert.deepEqual(costLoggerStub.getCall(2).args[0], { ConsumedCapacity: { ReadCapacityUnits: 59, GlobalSecondaryIndexes: null, LocalSecondaryIndexes: null, Time: 0 } }, 'costLoggerStub is called');
    assert.notOk(costLoggerStub.getCall(3), 'called 2 times');
    assert.end();
    timeStub.restore();
  });
});
dynamodb.delete();
dynamodb.close();
