/* eslint-env es6 */
var test = require('tape');
var Dyno = require('..');
const _ = require('underscore');
const crypto = require('crypto');
const sinon = require('sinon');
const testTables = require('./test-tables');
const dynamodb = require('@mapbox/dynamodb-test')(test, 'dyno', testTables.idhash);

test('[index] invalid config', function(assert) {
  assert.throws(function() {
    Dyno({ region: 'us-east-1' });
  }, /table is required/, 'rejects config without table');

  assert.throws(function() {
    Dyno({ table: 'my-table' });
  }, /region is required/, 'rejects config without region');

  assert.end();
});

test('[index] expected properties', function(assert) {
  var dyno = Dyno({ table: 'my-table', region: 'us-east-1' });

  assert.equal(typeof dyno.config, 'object', 'exposes config object');
  assert.equal(typeof dyno.listTables, 'function', 'exposes listTables function');
  assert.equal(typeof dyno.describeTable, 'function', 'exposes describeTable function');
  assert.equal(typeof dyno.batchGetItem, 'function', 'exposes batchGetItem function');
  assert.equal(typeof dyno.batchWriteItem, 'function', 'exposes batchWriteItem function');
  assert.equal(typeof dyno.deleteItem, 'function', 'exposes deleteItem function');
  assert.equal(typeof dyno.getItem, 'function', 'exposes getItem function');
  assert.equal(typeof dyno.putItem, 'function', 'exposes putItem function');
  assert.equal(typeof dyno.query, 'function', 'exposes query function');
  assert.equal(typeof dyno.scan, 'function', 'exposes scan function');
  assert.equal(typeof dyno.updateItem, 'function', 'exposes updateItem function');
  assert.equal(typeof dyno.batchGetItemRequests, 'function', 'exposes batchGetItemRequests function');
  assert.equal(typeof dyno.batchWriteItemRequests, 'function', 'exposes batchWriteItemRequests function');
  assert.equal(typeof dyno.batchGetAll, 'function', 'exposes batchGetAll function');
  assert.equal(typeof dyno.batchWriteAll, 'function', 'exposes batchWriteAll function');
  assert.equal(typeof dyno.createTable, 'function', 'exposes createTable function');
  assert.equal(typeof dyno.deleteTable, 'function', 'exposes deleteTable function');
  assert.equal(typeof dyno.queryStream, 'function', 'exposes queryStream function');
  assert.equal(typeof dyno.scanStream, 'function', 'exposes scanStream function');
  assert.equal(typeof dyno.putStream, 'function', 'exposes putStream function');

  var read = Dyno({ table: 'my-table', region: 'us-east-1', read: true });

  assert.equal(typeof read.config, 'object', 'read-only client exposes config object');
  assert.equal(typeof read.listTables, 'function', 'read-only client exposes listTables function');
  assert.equal(typeof read.listTablesAsync, 'function', 'read-only client exposes listTablesAsync function');

  assert.equal(typeof read.describeTable, 'function', 'read-only client exposes describeTable function');
  assert.equal(typeof read.describeTableAsync, 'function', 'read-only client exposes describeTableAsync function');
  
  assert.equal(typeof read.batchGetItem, 'function', 'read-only client exposes batchGetItem function');
  assert.equal(typeof read.batchGetItemAsync, 'function', 'read-only client exposes batchGetItemAsync function');
  
  assert.equal(typeof read.batchWriteItem, 'undefined', 'read-only client does not expose batchWriteItem function');
  
  assert.equal(typeof read.deleteItem, 'undefined', 'read-only client does not expose deleteItem function');
  
  assert.equal(typeof read.getItem, 'function', 'read-only client exposes getItem function');
  assert.equal(typeof read.getItemAsync, 'function', 'read-only client exposes getItemAsync function');
  
  assert.equal(typeof read.putItem, 'undefined', 'read-only client does not expose putItem function');
  
  assert.equal(typeof read.query, 'function', 'read-only client exposes query function');
  assert.equal(typeof read.queryAsync, 'function', 'read-only client exposes queryAsync function');
  
  assert.equal(typeof read.scan, 'function', 'read-only client exposes scan function');
  assert.equal(typeof read.scanAsync, 'function', 'read-only client exposes scanAsync function');
  
  assert.equal(typeof read.updateItem, 'undefined', 'read-only client does not expose updateItem function');
  
  assert.equal(typeof read.batchGetItemRequests, 'function', 'read-only client exposes batchGetItemRequests function');
  assert.equal(typeof read.batchWriteItemRequests, 'undefined', 'read-only client does not expose batchWriteItemRequests function');
  assert.equal(typeof read.batchGetAll, 'function', 'read-only client exposes batchGetAll function');
  assert.equal(typeof read.batchWriteAll, 'undefined', 'read-only client does not expose batchWriteAll function');
  
  assert.equal(typeof read.createTable, 'function', 'read-only client exposes createTable function');
  assert.equal(typeof read.createTableAsync, 'function', 'read-only client exposes createTableAsync function');
  
  assert.equal(typeof read.deleteTable, 'function', 'read-only client exposes deleteTable function');
  assert.equal(typeof read.deleteTableAsync, 'function', 'read-only client exposes deleteTableAsync function');
  
  assert.equal(typeof read.queryStream, 'function', 'read-only client exposes queryStream function');
  assert.equal(typeof read.scanStream, 'function', 'read-only client exposes scanStream function');
  assert.equal(typeof read.putStream, 'undefined', 'read-only client does not expose putStream function');
  

  var write = Dyno({ table: 'my-table', region: 'us-east-1', write: true });

  assert.equal(typeof write.config, 'object', 'write-only client exposes config object');
  assert.equal(typeof write.listTables, 'function', 'write-only client exposes listTables function');
  assert.equal(typeof write.listTablesAsync, 'function', 'write-only client exposes listTablesAsync function');

  assert.equal(typeof write.describeTable, 'function', 'write-only client exposes describeTable function');
  assert.equal(typeof write.describeTableAsync, 'function', 'write-only client exposes describeTableAsync function');

  assert.equal(typeof write.batchGetItem, 'undefined', 'write-only client does not expose batchGetItem function');
  assert.equal(typeof write.batchWriteItem, 'function', 'write-only client exposes batchWriteItem function');

  assert.equal(typeof write.deleteItem, 'function', 'write-only client exposes deleteItem function');
  assert.equal(typeof write.deleteItemAsync, 'function', 'write-only client exposes deleteItemAsync function');

  assert.equal(typeof write.getItem, 'undefined', 'write-only client does not expose getItem function');

  assert.equal(typeof write.putItem, 'function', 'write-only client exposes putItem function');
  assert.equal(typeof write.putItemAsync, 'function', 'write-only client exposes putItemAsync function');

  assert.equal(typeof write.query, 'undefined', 'write-only client does not expose query function');
  assert.equal(typeof write.scan, 'undefined', 'write-only client does not expose scan function');

  assert.equal(typeof write.updateItem, 'function', 'write-only client exposes updateItem function');
  assert.equal(typeof write.updateItemAsync, 'function', 'write-only client exposes updateItemAsync function');

  assert.equal(typeof write.batchGetItemRequests, 'undefined', 'write-only client does not expose batchGetItemRequests function');

  assert.equal(typeof write.batchWriteItemRequests, 'function', 'write-only client exposes batchWriteItemRequests function');
  assert.equal(typeof write.batchGetAll, 'undefined', 'write-only client does not expose batchGetAll function');
  assert.equal(typeof write.batchWriteAll, 'function', 'write-only client exposes batchWriteAll function');

  assert.equal(typeof write.createTable, 'function', 'write-only client exposes createTable function');
  assert.equal(typeof write.createTableAsync, 'function', 'write-only client exposes createTableAsync function');

  assert.equal(typeof write.deleteTable, 'function', 'write-only client exposes deleteTable function');
  assert.equal(typeof write.deleteTableAsync, 'function', 'write-only client exposes deleteTableAsync function');

  assert.equal(typeof write.queryStream, 'undefined', 'write-only client does not expose queryStream function');
  assert.equal(typeof write.scanStream, 'undefined', 'write-only client does not expose scanStream function');
  assert.equal(typeof write.putStream, 'function', 'write-only client exposes putStream function');

  var multi = Dyno.multi(
    { table: 'read-table', region: 'us-east-1' },
    { table: 'write-table', region: 'us-east-1' }
  );

  assert.equal(typeof multi.config, 'object', 'multi-client exposes config object');

  assert.equal(typeof multi.listTables, 'function', 'multi-client exposes listTables function');
  assert.equal(typeof multi.listTablesAsync, 'function', 'multi-client exposes listTablesAsync function');

  assert.equal(typeof multi.describeTable, 'function', 'multi-client exposes describeTable function');
  assert.equal(typeof multi.describeTableAsync, 'function', 'multi-client exposes describeTableAsync function');

  assert.equal(typeof multi.batchGetItem, 'function', 'multi-client exposes batchGetItem function');
  assert.equal(typeof multi.batchGetItemAsync, 'function', 'multi-client exposes batchGetItemAsync function');

  assert.equal(typeof multi.batchWriteItem, 'function', 'multi-client exposes batchWriteItem function');
  assert.equal(typeof multi.batchWriteItemAsync, 'function', 'multi-client exposes batchWriteItemAsync function');

  assert.equal(typeof multi.deleteItem, 'function', 'multi-client exposes deleteItem function');
  assert.equal(typeof multi.deleteItemAsync, 'function', 'multi-client exposes deleteItemAsync function');

  assert.equal(typeof multi.getItem, 'function', 'multi-client exposes getItem function');
  assert.equal(typeof multi.getItemAsync, 'function', 'multi-client exposes getItemAsync function');

  assert.equal(typeof multi.putItem, 'function', 'multi-client exposes putItem function');
  assert.equal(typeof multi.putItemAsync, 'function', 'multi-client exposes putItemAsync function');

  assert.equal(typeof multi.query, 'function', 'multi-client exposes query function');
  assert.equal(typeof multi.queryAsync, 'function', 'multi-client exposes queryAsync function');

  assert.equal(typeof multi.scan, 'function', 'multi-client exposes scan function');
  assert.equal(typeof multi.scanAsync, 'function', 'multi-client exposes scanAsync function');

  assert.equal(typeof multi.updateItem, 'function', 'multi-client exposes updateItem function');
  assert.equal(typeof multi.updateItemAsync, 'function', 'multi-client exposes updateItemAsync function');

  assert.equal(typeof multi.batchGetItemRequests, 'function', 'exposes batchGetItemRequests function');
  assert.equal(typeof multi.batchWriteItemRequests, 'function', 'exposes batchWriteItemRequests function');
  assert.equal(typeof multi.batchGetAll, 'function', 'exposes batchGetAll function');
  assert.equal(typeof multi.batchWriteAll, 'function', 'exposes batchWriteAll function');

  assert.equal(typeof multi.createTable, 'function', 'multi-client exposes createTable function');
  assert.equal(typeof multi.createTableAsync, 'function', 'multi-client exposes createTableAsync function');

  assert.equal(typeof multi.deleteTable, 'function', 'multi-client exposes deleteTable function');
  assert.equal(typeof multi.deleteTableAsync, 'function', 'multi-client exposes deleteTableAsync function');

  assert.equal(typeof multi.queryStream, 'function', 'multi-client exposes queryStream function');
  assert.equal(typeof multi.scanStream, 'function', 'multi-client exposes scanStream function');
  assert.equal(typeof multi.putStream, 'function', 'multi-client exposes putStream function');

  assert.end();
});

test('[index] module exposes static functions', function(assert) {
  assert.equal(typeof Dyno.createSet, 'function', 'exposes createSet function');
  assert.equal(typeof Dyno.serialize, 'function', 'exposes serialize function');
  assert.equal(typeof Dyno.deserialize, 'function', 'exposes deserialize function');
  assert.end();
});

test('[index] Dyno.createSet always yields a typed set', function(assert) {
  var convert = require('aws-sdk/lib/dynamodb/converter').input;

  assert.equal(Dyno.createSet(['a', 'b']).type, 'String', 'sets string type');
  assert.equal(Dyno.createSet([1, 2]).type, 'Number', 'sets number type');
  assert.equal(Dyno.createSet([new Buffer.from('hello')]).type, 'Binary', 'sets buffer type');
  assert.equal(Dyno.createSet(['']).type, 'String', 'sets string type on falsy value');
  assert.equal(Dyno.createSet([0]).type, 'Number', 'sets number type on falsy value');
  assert.deepEqual(convert(Dyno.createSet([0])), { NS: [ '0' ] }, 'set with falsy number value converts to appropriate wire-formatted object');
  assert.deepEqual(convert(Dyno.createSet([''])), { SS: [ '' ] }, 'set with falsy string value converts to appropriate wire-formatted object');
  assert.end();
});

test('[index] configuration', function(assert) {
  var config = {
    table: 'my-table',
    region: 'us-east-1',
    endpoint: 'http://localhost:4567',
    httpOptions: { timeout: 1000 },
    accessKeyId: 'access',
    secretAccessKey: 'secret',
    sessionToken: 'session',
    logger: console,
    maxRetries: 10,
    extra: 'crap'
  };

  var dyno = Dyno(config);

  assert.equal(dyno.config.region, config.region, 'sets region');
  assert.equal(dyno.config.endpoint, config.endpoint, 'sets endpoint');
  assert.deepEqual(dyno.config.params.TableName, config.table, 'sets params.TableName');
  assert.deepEqual(dyno.config.httpOptions, config.httpOptions, 'sets httpOptions');
  assert.equal(dyno.config.accessKeId, config.accessKeId, 'sets accessKeId');
  assert.equal(dyno.config.secretAccessKey, config.secretAccessKey, 'sets secretAccessKey');
  assert.equal(dyno.config.sessionToken, config.sessionToken, 'sets sessionToken');
  assert.deepEqual(dyno.config.logger, config.logger, 'sets logger');
  assert.equal(dyno.config.maxRetries, config.maxRetries, 'sets maxRetries');

  var multi = Dyno.multi(
    { table: 'read-table', region: 'us-east-1' },
    { table: 'write-table', region: 'us-east-1' }
  );

  assert.equal(multi.config.read.region, 'us-east-1', 'sets read region');
  assert.deepEqual(multi.config.read.params, { TableName: 'read-table' }, 'sets read params.TableName');
  assert.equal(multi.config.write.region, 'us-east-1', 'sets write region');
  assert.deepEqual(multi.config.write.params, { TableName: 'write-table' }, 'sets write params.TableName');

  assert.end();
});

test('[index] reuse client', function(assert) {
  const costLogger1 = sinon.stub();
  const costLogger2 = sinon.stub();
  const options = {
    table: 'my-table',
    region: 'us-east-1',
    endpoint: 'http://localhost:4567',
    costLogger: costLogger1
  };

  const dyno = Dyno(options);
  const dyno2 = Dyno({costLogger: costLogger2, dynoInstance: dyno});
  assert.equal(dyno._client, dyno2._client, 'client is reused');
  assert.equal(dyno.config, dyno2.config, 'config is reused');
  assert.end();
});

test('[index] reuse client in multi method', function(assert) {
  const costLogger1 = sinon.stub();
  const costLogger2 = sinon.stub();
  const readOptions = {
    table: 'my-table-read',
    region: 'us-east-1',
    endpoint: 'http://localhost:1234',
    costLogger: costLogger1
  };

  const writeOptions = {
    table: 'my-table-write',
    region: 'us-east-1',
    endpoint: 'http://localhost:4567',
    costLogger: costLogger1
  };


  const multiDyno = Dyno.multi(readOptions, writeOptions);
  const multiDyno2 = Dyno.multi(
    { costLogger: costLogger2, dynoInstance: multiDyno.read },
    { costLogger: costLogger2, dynoInstance: multiDyno.write }
  );
  assert.equal(multiDyno.config.read, multiDyno2.config.read, 'read config is reused');
  assert.equal(multiDyno.config.write, multiDyno2.config.write, 'write config is reused');
  assert.ok(multiDyno.read._client, 'read client is not empty');
  assert.ok(multiDyno.write._client, 'write client is not empty');
  assert.equal(multiDyno.read._client, multiDyno2.read._client, 'read client is reused');
  assert.equal(multiDyno.write._client, multiDyno2.write._client, 'write client is reused');
  assert.end();
});

test('[index] reject DynamoDB config when reusing client', function(assert) {
  const options = {
    table: 'my-table',
    region: 'us-east-1',
    endpoint: 'http://localhost:4567'
  };

  const dyno = Dyno(options);
  assert.throws(function() {
    Dyno({table: 'my-table', dynoInstance: dyno});
  }, /No need to provide DynamoDB config when reusing Dynamodb client/, 'rejects dynamodb config');
  assert.end();
});

dynamodb.start();

dynamodb.test('[index] promisified methods work', async function (assert) {
  const tableName = 'promisify-table';
  const dyno = Dyno({
    table: tableName,
    region: 'us-east-1',
    endpoint: 'http://localhost:4567',
  });
  const table = await dyno.createTableAsync(
    _({ TableName: tableName }).extend(testTables.idhash)
  );
  assert.ok(table.TableDefinition.TableArn, 'createTableAsync - table is created');
  const describedTable = await dyno.describeTableAsync({
    TableName: tableName,
  });
  assert.equal(
    describedTable.Table.TableId,
    table.TableDefinition.TableId,
    'describeTableAsync - table is described'
  );
  const tables = await dyno.listTablesAsync();
  assert.ok(tables.TableNames.includes(tableName), 'listTablesAsync - list the created table');
  const putResult = await dyno.putItemAsync({
    Item: {
      id: 'itemA',
    },
  });
  assert.ok(putResult, 'putItemAsync - item is created');
  const { Item } = await dyno.getItemAsync({ Key: { id: 'itemA' } });
  assert.equal(Item.id, 'itemA', 'getItemAsync - get the item');
  const updatedItem = await dyno.updateItemAsync({
    Key: { id: 'itemA' },
    UpdateExpression: 'set #name = :new',
    ExpressionAttributeNames: { '#name': 'name' },
    ExpressionAttributeValues: {
      ':new': 'new name',
    },
    ReturnValues: 'ALL_NEW',
  });
  assert.equal(updatedItem.Attributes.name, 'new name', 'updateItemAsync - item is updated');
  await dyno.putItemAsync({
    Item: {
      id: 'itemB',
    },
  });
  const batchGetResult = await dyno.batchGetItemAsync({
    RequestItems: {
      [tableName]: {
        Keys: [
          {
            id: 'itemA',
          },
          {
            id: 'itemB',
          },
        ],
      },
    },
  });
  assert.equal(
    batchGetResult.Responses[tableName].length,
    2,
    'batchGetItemAsync - get two items'
  );
  const batchWriteResult = await dyno.batchWriteItemAsync({
    RequestItems: {
      [tableName]: [
        {
          DeleteRequest: {
            Key: { id: 'itemA' },
          },
        },
        {
          PutRequest: {
            Item: { id: 'itemC' },
          },
        },
      ],
    },
    ReturnConsumedCapacity: 'INDEXES',
  });
  assert.equal(
    batchWriteResult.ConsumedCapacity[0].CapacityUnits,
    2,
    'batchWriteItemAsync - deleted 1 item and crated 1 item'
  );
  const queryResult = await dyno.queryAsync({
    KeyConditionExpression: 'id = :id',
    ExpressionAttributeValues: {
      ':id': 'itemC',
    }
  });
  assert.equal(queryResult.Items.length, 1, 'queryAsync - 1 item matched');
  const scanResult = await dyno.scanAsync();
  assert.equal(scanResult.Items.length, 2, 'scanResult - 2 items in the db');
  await dyno.deleteTableAsync({TableName: tableName});
  const afterDeletion = await dyno.listTablesAsync();
  assert.notOk(tables.TableNames.includes(afterDeletion), 'deleteTableAsync - table is deleted');
  assert.end();
});

dynamodb.test('different costLoggers are called', function(assert) {
  const costLogger1 = sinon.stub();
  const costLogger2 = sinon.stub();
  const options = {
    table: 'my-table',
    region: 'us-east-1',
    endpoint: 'http://localhost:4567',
    costLogger: costLogger1
  };
  function randomItems(num, bites) {
    return _.range(num).map(function(i) {
      return {
        id: i.toString(),
        range: i,
        data: crypto.randomBytes(bites || 36)
      };
    });
  }
  const records = randomItems(10);
  const params = { RequestItems: {} };
  params.RequestItems[dynamodb.tableName] = records.map(function(item) {
    return {
      PutRequest: { Item: item }
    };
  });
  const dyno = Dyno(options);
  const dyno2 = Dyno({costLogger: costLogger2, dynoInstance: dyno});
  
  dyno.batchWriteItem(params, function(err) {
    assert.notOk(err, 'no error');
    assert.ok(costLogger1.called, 'costLoggerStub1 is called');
    assert.notOk(costLogger2.called, 'costLoggerStub2 is not called');
    dyno2.batchWriteItem(params, function() {
      assert.notOk(err, 'no error');
      assert.ok(costLogger2.called, 'costLoggerStub2 is called');
      assert.ok(costLogger1.calledOnce, 'costLoggerStub1 is called only once');
      assert.end();
    });
  });
});

dynamodb.delete();
dynamodb.close();

