var Readable = require('stream').Readable;
var _ = require('underscore');
var parallel = require('parallel-stream');
var reduceCapacity = require('./util').reduceCapacity;

module.exports = function(client, tableName) {
  var stream = {};

  function readableStream(requestType, params, options) {
    options = options || {};
    var pages = options.pages || Infinity;
    var readable = new Readable(_({ objectMode: true }).defaults(options));
    var pending = false;
    var items = [];
    var started = false;

    readable.Count = 0;
    readable.ScannedCount = 0;

    function makeRequest(requestParams) {
      pending = true;

      // Validate the request early
      try {
        readable.emit('validate', requestParams);
      } catch (err) {
        return readable.emit('error', err);
      }

      client[requestType](requestParams)
        .then(response => {
          pending = false;

          readable.Count += response.Count || 0;
          readable.ScannedCount += response.ScannedCount || 0;
          readable.LastEvaluatedKey = response.LastEvaluatedKey;

          if (response.ConsumedCapacity) {
            if (!readable.ConsumedCapacity) readable.ConsumedCapacity = {};
            reduceCapacity(readable.ConsumedCapacity, response.ConsumedCapacity);
          }

          pages--;

          if (response.Items) {
            response.Items.forEach(function(item) { items.push(item); });
          }
          
          readable._read();
        })
        .catch(err => {
          pending = false;
          readable.emit('error', err);
        });
    }

    readable._read = function() {
      var status = true;
      while (status && items.length) status = readable.push(items.shift());
      if (items.length) return;
      
      // If we haven't started yet, start the first request
      if (!started) {
        started = true;
        return makeRequest(params);
      }
      
      // Check if we should continue pagination
      if (pages <= 0 || !readable.LastEvaluatedKey) {
        return readable.push(null); // End stream
      }
      
      if (status && !pending) {
        const nextParams = Object.assign({}, params, {
          ExclusiveStartKey: readable.LastEvaluatedKey,
        });
        makeRequest(nextParams);
      }
    };

    return readable;
  }

  stream.query = function(params, options) {
    return readableStream('query', params, options);
  };

  stream.scan = function(params, options) {
    return readableStream('scan', params, options);
  };

  stream.put = function(options) {
    options = options || {};
    var params = { RequestItems: {} };
    params.RequestItems[tableName] = [];

    function batchWrite(params, callback) {
      putStream.requestCount++;
      
      client.batchWrite(params)
        .then(data => {
          if (!data.UnprocessedItems || !Object.keys(data.UnprocessedItems).length) {
            return callback();
          }
          batchWrite({ RequestItems: data.UnprocessedItems }, callback);
        })
        .catch(err => callback(err));
    }

    function write(item, enc, callback) {
      params.RequestItems[tableName].push({ PutRequest: { Item: item } });
      if (params.RequestItems[tableName].length !== 25) return callback();

      batchWrite(params, callback);
      params.RequestItems[tableName] = [];
    }

    function flush(callback) {
      if (!params.RequestItems[tableName].length) return callback();
      batchWrite(params, callback);
    }

    var putStream = parallel.writable(write, flush, _(options).extend({ objectMode: true }));
    putStream.requestCount = 0;

    return putStream;
  };

  return stream;
};
