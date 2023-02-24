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

/**
 * Cast CapacityUnits to ReadCapacityUnits or WriteCapacityUnits based on the cast type
 * @param {Object} indexes  GlobalSecondaryIndexes or LocalSecondaryIndexes see https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_ConsumedCapacity.html#DDB-Type-ConsumedCapacity-GlobalSecondaryIndexes
 * @param {string} castKey ReadCapacityUnits | WriteCapacityUnits
 * 
 */
function castIndexesCapacity (indexes, castKey) {
  if (!indexes) return null;
  return Object.fromEntries(Object.keys(indexes).map(function(key) {
    return [key, {[castKey]: indexes[key][castKey] || indexes[key].CapacityUnits}];
  }));
}

/**
 * Call the costLogger function configured in the options with consumed CapacityUnits 
 * @param {object} res The response of dynamoDB request
 * @param {string} type Read | Write
 * @param {function} costLogger configured in the options
 * @param {number} Time the time consumed
 */
function callCostLogger (res, type, costLogger, Time) {
  if (!costLogger) return;
  if (res && res.ConsumedCapacity) {
    let ConsumedCapacity = {};
    reduceCapacity(ConsumedCapacity, res.ConsumedCapacity);
    const castKey = `${type}CapacityUnits`;
    costLogger({
      ConsumedCapacity: {
        CapacityUnits: ConsumedCapacity.CapacityUnits,
        [castKey]: ConsumedCapacity[castKey] || ConsumedCapacity.CapacityUnits || 0,
        GlobalSecondaryIndexes: castIndexesCapacity(ConsumedCapacity.GlobalSecondaryIndexes, castKey),
        LocalSecondaryIndexes: castIndexesCapacity(ConsumedCapacity.LocalSecondaryIndexes, castKey),
      },
      Time
    });
  }
}

/**
 * Wrap the native method of DynamoDB to call the costLogger in the callback.
 * If no costLogger is provided, just return the native method directly
 * 
 * @param {function} costLogger The function that will be called with casted consumedCapacity
 * @param {function} nativeMethod The native method of dynamoDB
 * @param {string} type Indicate the method consumes Read or Write capacity
 */

function requestHandler(costLogger, nativeMethod, type) {
  if (!costLogger) {
    return nativeMethod;
  }
  if (!['Write', 'Read'].includes(type)) {
    throw new Error('Invalid capacity type');
  }
  return function(params, callback) {
    params.ReturnConsumedCapacity = 'INDEXES';
    const start = Date.now();
    if (callback) {
      return nativeMethod(params, function (err, res) {
        const Time = Date.now() - start;
        callCostLogger(res, type, costLogger, Time);
        callback && callback(err, res);
      });
    }
    const request =  nativeMethod(params);
    const send = request.send.bind(request);
    request.send = (callbackOfSend) => {
      const sendStart = Date.now();
      send((err, res) => {
        const Time = Date.now() - sendStart;
        callCostLogger(res, type, costLogger, Time);
        callbackOfSend && callbackOfSend(err, res);
      });
    };
    return request;
  };
}

/**
 * Get if a method is a write or read method
 * @param {string} fnName 
 * @returns Write|Read
 */
function getMethodType (fnName) {
  return [
    'batchWrite',
    'delete',
    'put',
    'update',
  ].includes(fnName) ? 'Write' : 'Read';
}

/**
 * Wrap all methods used of DynamoDB DocumentClient with requestHandler
 * @param {object} client DynamoDB DocumentClient
 * @param {function} costLogger The function that will be called with casted consumedCapacity
 * @returns 
 */
function wrapDocClient (client, costLogger) {
  const methods = ['batchGet', 'batchWrite', 'put', 'get', 'update', 'delete', 'query', 'scan'];
  return Object.fromEntries(
    methods.map(function (m) {
      return [
        m,
        requestHandler(costLogger, client[m].bind(client), getMethodType(m)),
      ];
    })
  );
}

module.exports = {
  reduceCapacity,
  requestHandler,
  wrapDocClient,
  castIndexesCapacity
};
