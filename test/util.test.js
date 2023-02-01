/* eslint-env es6 */
const test = require('tape');
const reduceCapacity = require('../lib/util').reduceCapacity;
const requestHandler = require('../lib/util').requestHandler;
const wrapClient = require('../lib/util').wrapClient;
const wrapDocClient = require('../lib/util').wrapDocClient;
const sinon = require('sinon');

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

test ('[requestHandler] costLogger is called with consumedCapacity', function (assert) {
  const costLogger = sinon.stub();
  const nativeMethod = function (params, callback) {
    callback(null, {
      ConsumedCapacity: 100
    });
  };
  const wrappedMethod = requestHandler(costLogger, nativeMethod, 'Write');
  wrappedMethod({}, function(){});
  assert.ok(costLogger.calledOnceWith({ WriteConsumedCapacity: 100 }), 'Get correct consumedCapacity');
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

test('[wrapClient] return correct method', function (assert) {
  const client = {
    listTables: function(param, callback) { callback(null, {ConsumedCapacity: 100}); },
    describeTable: function(param, callback) { callback(null, {ConsumedCapacity: 200}); },
    createTable: function(param, callback) { callback(null, {ConsumedCapacity: 300}); },
    deleteTable: function(param, callback) { callback(null, {ConsumedCapacity: 400}); },
    batchGetItem: function(param, callback) { callback(null, {ConsumedCapacity: 500}); },
    batchWriteItem: function(param, callback) { callback(null, {ConsumedCapacity: 600}); },
    deleteItem: function(param, callback) { callback(null, {ConsumedCapacity: 700}); },
    getItem: function(param, callback) { callback(null, {ConsumedCapacity: 800}); },
  };
  const costLoggerStub = sinon.stub();
  const wrappedClient = wrapClient(client, costLoggerStub);
  const callbackStub = sinon.stub();
  
  wrappedClient.listTables({}, callbackStub);
  assert.ok(callbackStub.called, 'callback of listTables is called');
  assert.ok(costLoggerStub.calledWith({ ReadConsumedCapacity: 100 }), 'costLoggerStub is called');

  wrappedClient.describeTable({}, callbackStub);
  assert.ok(callbackStub.called, 'callback of describeTable is called');
  assert.ok(costLoggerStub.calledWith({ ReadConsumedCapacity: 200 }), 'costLoggerStub is called');

  wrappedClient.createTable({}, callbackStub);
  assert.ok(callbackStub.called, 'callback of createTable is called');
  assert.ok(costLoggerStub.calledWith({ WriteConsumedCapacity: 300 }), 'costLoggerStub is called');
  
  wrappedClient.deleteTable({}, callbackStub);
  assert.ok(callbackStub.called, 'callback of deleteTable is called');
  assert.ok(costLoggerStub.calledWith({ WriteConsumedCapacity: 400 }), 'costLoggerStub is called');
  
  wrappedClient.batchGetItem({}, callbackStub);
  assert.ok(callbackStub.called, 'callback of batchGetItem is called');
  assert.ok(costLoggerStub.calledWith({ ReadConsumedCapacity: 500 }), 'costLoggerStub is called');
  
  wrappedClient.batchWriteItem({}, callbackStub);
  assert.ok(callbackStub.called, 'callback of batchWriteItem is called');
  assert.ok(costLoggerStub.calledWith({ WriteConsumedCapacity: 600 }), 'costLoggerStub is called');
  
  wrappedClient.deleteItem({}, callbackStub);
  assert.ok(callbackStub.called, 'callback of deleteItem is called');
  assert.ok(costLoggerStub.calledWith({ WriteConsumedCapacity: 700 }), 'costLoggerStub is called');

  wrappedClient.getItem({}, callbackStub);
  assert.ok(callbackStub.called, 'callback of getItem is called');
  assert.ok(costLoggerStub.calledWith({ ReadConsumedCapacity: 800 }), 'costLoggerStub is called');

  assert.end();
});

test('[wrapDocClient] return correct method', function (assert) {
  const docClient = {
    batchGet: function(param, callback) { callback(null, {ConsumedCapacity: 100}); },
    batchWrite: function(param, callback) { callback(null, {ConsumedCapacity: 200}); },
    get: function(param, callback) { callback(null, {ConsumedCapacity: 300}); },
    update: function(param, callback) { callback(null, {ConsumedCapacity: 400}); },
    put: function(param, callback) { callback(null, {ConsumedCapacity: 500}); },
    delete: function(param, callback) { callback(null, {ConsumedCapacity: 600}); },
    query: function(param, callback) { callback(null, {ConsumedCapacity: 700}); },
    scan: function(param, callback) { callback(null, {ConsumedCapacity: 800}); },
  };
  const costLoggerStub = sinon.stub();
  const wrappedClient = wrapDocClient(docClient, costLoggerStub);
  const callbackStub = sinon.stub();
  
  wrappedClient.batchGet({}, callbackStub);
  assert.ok(callbackStub.called, 'callback of batchGet is called');
  assert.ok(costLoggerStub.calledWith({ ReadConsumedCapacity: 100 }), 'costLoggerStub is called');

  wrappedClient.batchWrite({}, callbackStub);
  assert.ok(callbackStub.called, 'callback of batchWrite is called');
  assert.ok(costLoggerStub.calledWith({ WriteConsumedCapacity: 200 }), 'costLoggerStub is called');

  wrappedClient.get({}, callbackStub);
  assert.ok(callbackStub.called, 'callback of get is called');
  assert.ok(costLoggerStub.calledWith({ ReadConsumedCapacity: 300 }), 'costLoggerStub is called');
  
  wrappedClient.update({}, callbackStub);
  assert.ok(callbackStub.called, 'callback of update is called');
  assert.ok(costLoggerStub.calledWith({ WriteConsumedCapacity: 400 }), 'costLoggerStub is called');

  wrappedClient.put({}, callbackStub);
  assert.ok(callbackStub.called, 'callback of put is called');
  assert.ok(costLoggerStub.calledWith({ WriteConsumedCapacity: 500 }), 'costLoggerStub is called');
  
  wrappedClient.delete({}, callbackStub);
  assert.ok(callbackStub.called, 'callback of delete is called');
  assert.ok(costLoggerStub.calledWith({ WriteConsumedCapacity: 600 }), 'costLoggerStub is called');
  
  
  wrappedClient.query({}, callbackStub);
  assert.ok(callbackStub.called, 'callback of query is called');
  assert.ok(costLoggerStub.calledWith({ ReadConsumedCapacity: 700 }), 'costLoggerStub is called');
  
  wrappedClient.scan({}, callbackStub);
  assert.ok(callbackStub.called, 'callback of scan is called');
  assert.ok(costLoggerStub.calledWith({ ReadConsumedCapacity: 800 }), 'costLoggerStub is called');

  assert.end();
});
