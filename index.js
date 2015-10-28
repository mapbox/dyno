var AWS = require('aws-sdk');
var _ = require('underscore');

module.exports = Dyno;

function Dyno(options) {
  if (!options.table) throw new Error('table is required'); // Demand table be specified
  if (!options.region) throw new Error('region is required');

  var config = {
    region: options.region,
    endpoint: options.endpoint,
    params: { TableName: options.table }, // Sets `TableName` in every request
    httpOptions: options.httpOptions || { timeout: 5000 }, // Default appears to be 2 min
    accessKeyId: options.accessKeyId,
    secretAccessKey: options.secretAccessKey,
    sessionToken: options.sessionToken,
    logger: options.logger,
    maxRetries: options.maxRetries
  };

  var client = new AWS.DynamoDB(config);
  var docClient = new AWS.DynamoDB.DocumentClient({ service: client });
  var tableFreeClient = new AWS.DynamoDB(_(config).omit('params')); // no TableName in batch requests
  var tableFreeDocClient = new AWS.DynamoDB.DocumentClient({ service: tableFreeClient });

  // Straight-up inherit several functions from aws-sdk so we can also inherit docs and tests
  var nativeFunctions = {
    listTables: client.listTables.bind(client),
    describeTable: client.describeTable.bind(client),
    batchGetItem: tableFreeDocClient.batchGet.bind(tableFreeDocClient),
    batchWriteItem: tableFreeDocClient.batchWrite.bind(tableFreeDocClient),
    deleteItem: docClient.delete.bind(docClient),
    getItem: docClient.get.bind(docClient),
    putItem: docClient.put.bind(docClient),
    query: docClient.query.bind(docClient),
    scan: docClient.scan.bind(docClient),
    updateItem: docClient.update.bind(docClient)
  };

  var dynoExtensions = {
    batchGetItemRequests: require('./lib/requests')(docClient).batchGetItemRequests, // provide an array of AWS.Requests
    batchWriteItemRequests: require('./lib/requests')(docClient).batchWriteItemRequests, // provide an array of AWS.Requests
    createTable: require('./lib/table')(client, null).create, // to poll until table is ready
    deleteTable: require('./lib/table')(client).delete, // to poll until table is gone
    queryStream: require('./lib/stream')(docClient).query, // provide a readable stream
    scanStream: require('./lib/stream')(docClient).scan // provide a readable stream
  };

  // Drop specific functions from read/write only clients
  if (options.read) {
    delete nativeFunctions.deleteItem;
    delete nativeFunctions.putItem;
    delete nativeFunctions.updateItem;
    delete nativeFunctions.batchWriteItem;
    delete dynoExtensions.batchWriteItemRequests;
  }

  if (options.write) {
    delete nativeFunctions.batchGetItem;
    delete nativeFunctions.getItem;
    delete nativeFunctions.query;
    delete nativeFunctions.scan;
    delete dynoExtensions.batchGetItemRequests;
    delete dynoExtensions.queryStream;
    delete dynoExtensions.scanStream;
  }

  // Glue everything together
  return _({ config: config }).extend(nativeFunctions, dynoExtensions);
}

Dyno.multi = function(readOptions, writeOptions) {
  var read = Dyno(_({}).extend(readOptions, { read: true }));
  var write = Dyno(_({}).extend(writeOptions, { write: true }));

  return _({}).extend(write, read, {
    config: { read: read.config, write: write.config },
    createTable: require('./lib/table')(read, write).multiCreate,
    deleteTable: require('./lib/table')(read, write).multiDelete
  });
};

Dyno.createSet = function(list) {
  var DynamoDBSet = require('aws-sdk/lib/dynamodb/set');
  return new DynamoDBSet(list);
};

Dyno.serialize = require('./lib/serialization').serialize;
Dyno.deserialize = require('./lib/serialization').deserialize;
