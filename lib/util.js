/* eslint-env es6 */
/**
 * Reduce two sets of consumed capacity metrics into a single object
 * This should be in sync with Callback Parameters section of
 * https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#query-property
 *
 * @param {object} existing capacity. This object will be updated.
 * @param {object | array} new capacity object(s) to be added to the existing object.
 */
function reduceCapacity(existing, incoming) {
  if (!existing) {
    return;
  }

  if (Array.isArray(incoming)) {
    for (const item of incoming) {
      reduceCapacity(existing, item);
    }
    return;
  }

  function mergeCapacityUnits(dst, src) {
    if (src.CapacityUnits) {
      dst.CapacityUnits = (dst.CapacityUnits || 0) + src.CapacityUnits;
    }
    if (src.ReadCapacityUnits) {
      dst.ReadCapacityUnits = (dst.ReadCapacityUnits || 0) + src.ReadCapacityUnits;
    }
    if (src.WriteCapacityUnits) {
      dst.WriteCapacityUnits = (dst.WriteCapacityUnits || 0) + src.WriteCapacityUnits;
    }
  }

  function mergeCapacityParents(dst, src, k) {
    if (!src[k]) {
      return;
    }
    dst[k] = dst[k] || {};
    mergeCapacityUnits(dst[k], src[k]);
  }

  existing.TableName = existing.TableName || incoming.TableName;

  mergeCapacityUnits(existing, incoming);
  mergeCapacityParents(existing, incoming, 'Table');
  mergeCapacityParents(existing, incoming, 'LocalSecondaryIndexes');
  mergeCapacityParents(existing, incoming, 'GlobalSecondaryIndexes');

  for (const indexGroup of [
    'LocalSecondaryIndexes',
    'GlobalSecondaryIndexes',
  ]) {
    const dst = existing[indexGroup];
    const src = incoming[indexGroup];
    for (const index of Object.keys(src || {})) {
      mergeCapacityParents(dst, src, index);
    }
  }
}

function requestHandler(costLogger, nativeMethod, type) {
  if (!costLogger) {
    return nativeMethod;
  }
  if (!['Write', 'Read'].includes(type)) {
    throw new Error('Invalid capacity type');
  }
  return function(params, callback) {
    params.ReturnConsumedCapacity = 'INDEXES';
    nativeMethod(params, function (err, res) {
      callback(err, res);
      if (res && res.ConsumedCapacity) {
        costLogger({
          [`${type}ConsumedCapacity`]: res.ConsumedCapacity
        });
      }
    });
  };
}

/**
 * Get if a method is a write or read method
 * @param {string} fnName 
 * @returns Write|Read
 */
function getMethodType (fnName) {
  return [
    'deleteTable',
    'batchWriteItem',
    'deleteItem',
    'createTable',
    'put',
    'delete',
    'update',
    'batchWrite'
  ].includes(fnName) ? 'Write' : 'Read';
}

function wrapClient (client, costLogger) {
  return {
    listTables: requestHandler(costLogger, client.listTables.bind(client), getMethodType('listTables')),
    describeTable: requestHandler(costLogger, client.describeTable.bind(client), getMethodType('describeTable')),
    createTable: requestHandler(costLogger, client.createTable.bind(client), getMethodType('createTable')),
    deleteTable: requestHandler(costLogger, client.deleteTable.bind(client), getMethodType('deleteTable')),
    batchGetItem: requestHandler(costLogger, client.batchGetItem.bind(client), getMethodType('batchGetItem')),
    batchWriteItem: requestHandler(costLogger, client.batchWriteItem.bind(client), getMethodType('batchWriteItem')),
    deleteItem: requestHandler(costLogger, client.deleteItem.bind(client), getMethodType('deleteItem')),
    getItem: requestHandler(costLogger, client.getItem.bind(client), getMethodType('getItem')),
  };
}

function wrapDocClient (client, costLogger) {
  return {
    batchGet: requestHandler(costLogger, client.batchGet.bind(client), getMethodType('batchGet')),
    batchWrite: requestHandler(costLogger, client.batchWrite.bind(client), getMethodType('batchWrite')),
    put: requestHandler(costLogger, client.put.bind(client), getMethodType('put')),
    get: requestHandler(costLogger, client.get.bind(client), getMethodType('get')),
    update: requestHandler(costLogger, client.update.bind(client), getMethodType('update')),
    delete: requestHandler(costLogger, client.delete.bind(client), getMethodType('delete')),
    query: requestHandler(costLogger, client.query.bind(client), getMethodType('query')),
    scan: requestHandler(costLogger, client.scan.bind(client), getMethodType('scan'))
  };
}

module.exports = {
  reduceCapacity,
  requestHandler,
  wrapClient,
  wrapDocClient
};
