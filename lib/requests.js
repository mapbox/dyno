var big = require('big.js');
var converter = require('aws-sdk/lib/dynamodb/converter');
var queue = require('queue-async');
var reduceCapacity = require('./util').reduceCapacity;
var createBulkWriteRequests = require('./create_bulk_write_requests');
var createBulkGetRequests = require('./create_bulk_get)requests');


module.exports = function(client) {
  var api = {
    batchWriteItemRequests: function(params) {
      var requestSet = createBulkWriteRequests(params);
      requestSet.sendAll = function(concurrency, callback) {
        sendAll(client, requestSet, 'batchWrite', concurrency, callback);
      }
    },
    batchGetItemRequests: function(params) {
      var requestSet = createBulkGetRequests(params);
      requestSet.sendAll = function(concurrency, callback) {
        sendAll(client, requestSet, 'batchGet', concurrency, callback);
      }
    },
    batchWriteAll: function(params, maxRetryCount) {
      maxRetryCount = maxRetryCount || 10;
      var requestSet = requests.batchWriteItemRequests(params);
      requestSet.sendAll = function(concurrency, callback) {
        sendCompletely(client, requestSet, 'batchWrite', maxRetryCount, concurrency, callback);
      }
      return requestSet;
    },
    batchGetAll: function(params, maxRetryCount) {
      maxRetryCount = maxRetryCount || 10;
      var requestSet = requests.batchGetItemRequests(params);
      requestSet.sendAll = function(concurrency, callback) {
        sendCompletely(client, requestSet, 'batchGet', maxRetryCount, concurrency, callback);
      }
      return requestSet;
    }
  };

  return api;
}

function sendCompletely(client, requests, fnName, maxRetryCount, concurrency, callback) {

}

function sendAll (client, requests, fnName, concurrency, callback) {

}

/***
OLD

module.exports = function(client) {
  var requests = {};

  /**
   * Given a set of requests, this function sends them all at specified concurrency.
   * Generally, this function is not called directly, but is bound to an array of
   * requests with the first two parameters wired to specific values.
   *
   * @private
   * @param {RequestSet} requests - an array of AWS.Request objects
   * @param {string} fnName - the name of the aws-sdk client function that will
   * be used to construct new requests should an unprocessed items be detected
   * @param {number} concurrency - the concurrency with which batch requests
   * will be made
   * @param {function} callback - a function to handle the response
   */
  function sendAll(requests, fnName, concurrency, callback) {
    if (typeof concurrency === 'function') {
      callback = concurrency;
      concurrency = 1;
    }

    var q = queue(concurrency);

    requests.forEach(function(req) {
      q.defer(function(next) {
        if (!req) return next();
        req.on('complete', function(response) {
          next(null, response);
        }).send();
      });
    });

    q.awaitAll(function(err, responses) {
      if (err) return callback(err);

      var errors = [];
      var data = [];
      var unprocessed = [];

      responses.forEach(function(response) {
        if (!response) response = { error: null, data: null };

        errors.push(response.error);
        data.push(response.data);

        if (!response.data) return unprocessed.push(null);

        var newParams = {
          RequestItems: response.data.UnprocessedItems || response.data.UnprocessedKeys
        };

        if (newParams.RequestItems && !Object.keys(newParams.RequestItems).length)
          return unprocessed.push(null);

        unprocessed.push(newParams.RequestItems ? newParams : null);
      });

      unprocessed = unprocessed.map(function(params) {
        if (params) return client[fnName].bind(client)(params);
        else return null;
      });

      var unprocessedCount = unprocessed.filter(function(req) { return !!req; }).length;
      if (unprocessedCount) unprocessed.sendAll = sendAll.bind(null, unprocessed, fnName);

      var errorCount = errors.filter(function(err) { return !!err; }).length;

      callback(
        errorCount ? errors : null,
        data,
        unprocessedCount ? unprocessed : null
      );
    });
  }

  function appendDataToResult(result, response, attribute) {
    result.data[attribute] = result.data[attribute] || {};
    Object.keys(response.data[attribute]).forEach(function(table) {
      if (attribute === 'UnprocessedKeys') {
        result.data[attribute][table] = result.data[attribute][table] || { Keys: [] };
        result.data[attribute][table].Keys =  result.data[attribute][table].Keys.concat(response.data[attribute][table].Keys);
      } else {
        result.data[attribute][table] = (result.data[attribute][table] || []).concat(response.data[attribute][table]);
      }
    });
    return result;
  }

  /**
   * Given a set of requests, this function sends them all at specified concurrency,
   * retrying any unprocessed items until every request has either succeeded or failed.
   * The responses from the set of requests are aggregated into a single response.
   * Generally, this function is not called directly, but is bound to an array of
   * requests with the first two parameters wired to specific values.
   *
   * @private
   * @param {RequestSet} requests - an array of AWS.Request objects
   * @param {string} fnName - the name of the aws-sdk client function that will
   * be used to construct new requests should an unprocessed items be detected
   * @param {number} concurrency - the concurrency with which batch requests
   * @param {number} maxRetryCount - the number of times to retry
   * unprocessed items. 10 is what Amazon recomends.
   * will be made
   * @param {function} callback - a function to handle the response
   */
  function sendCompletely(requests, fnName, maxRetryCount, concurrency, callback) {
    if (typeof concurrency === 'function') {
      callback = concurrency;
      concurrency = 1;
    }

    function sendOne(req, callback) {
      if (!req) return callback();
      var result = { error: undefined, data: {} };

      var retryCount = 0;

      (function send(req) {
        req.on('complete', function(response) {
          if (!response.data) {
            result.error = response.error;
            return callback(null, result);
          }

          if (response.data.Responses) {
            result = appendDataToResult(result, response, 'Responses');
          }

          if (response.data.ConsumedCapacity) {
            result.data.ConsumedCapacity = result.data.ConsumedCapacity || {};
            reduceCapacity(result.data.ConsumedCapacity, response.data.ConsumedCapacity);
          }

          // If there is nothing left to process, resolve
          var unprocessedData = response.data.UnprocessedItems || response.data.UnprocessedKeys || {};
          if (Object.keys(unprocessedData).length === 0) {
            return callback(null, result);
          }

          retryCount++;

          // If we're about to retry more times than the desired max
          // note error and exit
          if (retryCount > maxRetryCount) {
            if (response.data.UnprocessedItems) {
              result = appendDataToResult(result, response, 'UnprocessedItems');
            }
            if (response.data.UnprocessedKeys) {
              result = appendDataToResult(result, response, 'UnprocessedKeys');
            }
            return callback(null, result);
          }

          // retry
          var delay = 50 * Math.pow(2, retryCount - 1);
          setTimeout(function() {
            send(client[fnName]({
              RequestItems: response.data.UnprocessedItems || response.data.UnprocessedKeys,
              ReturnConsumedCapacity: req.params.ReturnConsumedCapacity
            }));
          }, delay);

        }).send();
      })(req);
    }

    var q = queue(concurrency);

    requests.forEach(function(req) {
      q.defer(sendOne, req);
    });

    q.awaitAll(function(_, responses) {
      var err;
      var result = { data: {Responses: {}} };
      responses.forEach(function(res) {
        if (res.error) err = res.error;

        if (res.data.Responses) {
          result = appendDataToResult(result, res, 'Responses');
        }

        if (res.data.UnprocessedItems) {
          result = appendDataToResult(result, res, 'UnprocessedItems');
        }

        if (res.data.UnprocessedKeys) {
          result = appendDataToResult(result, res, 'UnprocessedKeys');
        }

        if (res.data.ConsumedCapacity) {
          result.data.ConsumedCapacity = result.data.ConsumedCapacity || {};
          reduceCapacity(result.data.ConsumedCapacity, res.data.ConsumedCapacity);
        }
      });

      callback(err, result.data);
    });
  }





  return requests;
};


