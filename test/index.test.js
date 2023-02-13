var test = require('tape');
var Dyno = require('..');

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
  assert.equal(typeof read.describeTable, 'function', 'read-only client exposes describeTable function');
  assert.equal(typeof read.batchGetItem, 'function', 'read-only client exposes batchGetItem function');
  assert.equal(typeof read.batchWriteItem, 'undefined', 'read-only client does not expose batchWriteItem function');
  assert.equal(typeof read.deleteItem, 'undefined', 'read-only client does not expose deleteItem function');
  assert.equal(typeof read.getItem, 'function', 'read-only client exposes getItem function');
  assert.equal(typeof read.putItem, 'undefined', 'read-only client does not expose putItem function');
  assert.equal(typeof read.query, 'function', 'read-only client exposes query function');
  assert.equal(typeof read.scan, 'function', 'read-only client exposes scan function');
  assert.equal(typeof read.updateItem, 'undefined', 'read-only client does not expose updateItem function');
  assert.equal(typeof read.batchGetItemRequests, 'function', 'read-only client exposes batchGetItemRequests function');
  assert.equal(typeof read.batchWriteItemRequests, 'undefined', 'read-only client does not expose batchWriteItemRequests function');
  assert.equal(typeof read.batchGetAll, 'function', 'read-only client exposes batchGetAll function');
  assert.equal(typeof read.batchWriteAll, 'undefined', 'read-only client does not expose batchWriteAll function');
  assert.equal(typeof read.createTable, 'function', 'read-only client exposes createTable function');
  assert.equal(typeof read.deleteTable, 'function', 'read-only client exposes deleteTable function');
  assert.equal(typeof read.queryStream, 'function', 'read-only client exposes queryStream function');
  assert.equal(typeof read.scanStream, 'function', 'read-only client exposes scanStream function');
  assert.equal(typeof read.putStream, 'undefined', 'read-only client does not expose putStream function');

  var write = Dyno({ table: 'my-table', region: 'us-east-1', write: true });

  assert.equal(typeof write.config, 'object', 'write-only client exposes config object');
  assert.equal(typeof write.listTables, 'function', 'write-only client exposes listTables function');
  assert.equal(typeof write.describeTable, 'function', 'write-only client exposes describeTable function');
  assert.equal(typeof write.batchGetItem, 'undefined', 'write-only client does not expose batchGetItem function');
  assert.equal(typeof write.batchWriteItem, 'function', 'write-only client exposes batchWriteItem function');
  assert.equal(typeof write.deleteItem, 'function', 'write-only client exposes deleteItem function');
  assert.equal(typeof write.getItem, 'undefined', 'write-only client does not expose getItem function');
  assert.equal(typeof write.putItem, 'function', 'write-only client exposes putItem function');
  assert.equal(typeof write.query, 'undefined', 'write-only client does not expose query function');
  assert.equal(typeof write.scan, 'undefined', 'write-only client does not expose scan function');
  assert.equal(typeof write.updateItem, 'function', 'write-only client exposes updateItem function');
  assert.equal(typeof write.batchGetItemRequests, 'undefined', 'write-only client does not expose batchGetItemRequests function');
  assert.equal(typeof write.batchWriteItemRequests, 'function', 'write-only client exposes batchWriteItemRequests function');
  assert.equal(typeof write.batchGetAll, 'undefined', 'write-only client does not expose batchGetAll function');
  assert.equal(typeof write.batchWriteAll, 'function', 'write-only client exposes batchWriteAll function');
  assert.equal(typeof write.createTable, 'function', 'write-only client exposes createTable function');
  assert.equal(typeof write.deleteTable, 'function', 'write-only client exposes deleteTable function');
  assert.equal(typeof write.queryStream, 'undefined', 'write-only client does not expose queryStream function');
  assert.equal(typeof write.scanStream, 'undefined', 'write-only client does not expose scanStream function');
  assert.equal(typeof write.putStream, 'function', 'write-only client exposes putStream function');

  var multi = Dyno.multi(
    { table: 'read-table', region: 'us-east-1' },
    { table: 'write-table', region: 'us-east-1' }
  );

  assert.equal(typeof multi.config, 'object', 'multi-client exposes config object');
  assert.equal(typeof multi.listTables, 'function', 'multi-client exposes listTables function');
  assert.equal(typeof multi.describeTable, 'function', 'multi-client exposes describeTable function');
  assert.equal(typeof multi.batchGetItem, 'function', 'multi-client exposes batchGetItem function');
  assert.equal(typeof multi.batchWriteItem, 'function', 'multi-client exposes batchWriteItem function');
  assert.equal(typeof multi.deleteItem, 'function', 'multi-client exposes deleteItem function');
  assert.equal(typeof multi.getItem, 'function', 'multi-client exposes getItem function');
  assert.equal(typeof multi.putItem, 'function', 'multi-client exposes putItem function');
  assert.equal(typeof multi.query, 'function', 'multi-client exposes query function');
  assert.equal(typeof multi.scan, 'function', 'multi-client exposes scan function');
  assert.equal(typeof multi.updateItem, 'function', 'multi-client exposes updateItem function');
  assert.equal(typeof multi.batchGetItemRequests, 'function', 'exposes batchGetItemRequests function');
  assert.equal(typeof multi.batchWriteItemRequests, 'function', 'exposes batchWriteItemRequests function');
  assert.equal(typeof multi.batchGetAll, 'function', 'exposes batchGetAll function');
  assert.equal(typeof multi.batchWriteAll, 'function', 'exposes batchWriteAll function');
  assert.equal(typeof multi.createTable, 'function', 'multi-client exposes createTable function');
  assert.equal(typeof multi.deleteTable, 'function', 'multi-client exposes deleteTable function');
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
  var dyno2 = Dyno(config, dyno);
  assert.equal(dyno.client, dyno2.client, 'client is reused');
  assert.equal(dyno.config, dyno2.config, 'config is reused');
  assert.end();
});
