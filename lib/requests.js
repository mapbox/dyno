var big = require('big.js');
var converter = require('aws-sdk/lib/dynamodb/converter');
var queue = require('queue-async');
var reduceCapacity = require('./util').reduceCapacity;

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
   * @param {number} maxRetries - the number of times sendCompletly will
   * attempt to resend unprocssed items
   * @param {number} concurrency - the concurrency with which batch requests
   * will be made
   * @param {function} callback - a function to handle the response
   */
  function sendCompletely(requests, fnName, maxRetries, concurrency, callback) {
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
            result.data.Responses = result.data.Responses || {};
            Object.keys(response.data.Responses).forEach(function(table) {
              result.data.Responses[table] = result.data.Responses[table] || [];
              response.data.Responses[table].forEach(function(res) {
                result.data.Responses[table].push(res);
              });
            });
          }

          if (response.data.ConsumedCapacity) {
            result.data.ConsumedCapacity = result.data.ConsumedCapacity || {};
            reduceCapacity(result.data.ConsumedCapacity, response.data.ConsumedCapacity);
          }

          var newParams = {
            RequestItems: response.data.UnprocessedItems || response.data.UnprocessedKeys,
            ReturnConsumedCapacity: req.params.ReturnConsumedCapacity
          };

          if (newParams.RequestItems && Object.keys(newParams.RequestItems).length && retryCount < maxRetries) {
            var delay = 50 * Math.pow(2, retryCount - 1);
            retryCount++;
            return setTimeout(function() {
              send(client[fnName](newParams));
            }, delay);
          }


          if (response.data.UnprocessedKeys) {
            result.data.UnprocessedKeys = result.data.UnprocessedKeys || {};
            Object.keys(response.data.UnprocessedKeys).forEach(function(table) {
              result.data.UnprocessedKeys[table] = result.data.UnprocessedKeys[table] || { Keys: [] };
              response.data.UnprocessedKeys[table].Keys.forEach(function(res) {
                result.data.UnprocessedKeys[table].Keys.push(res);
              });
            });
          }

          if (response.data.UnprocessedItems) {
            result.data.UnprocessedItems = result.data.UnprocessedItems || {};
            Object.keys(response.data.UnprocessedItems).forEach(function(table) {
              result.data.UnprocessedItems[table] = result.data.UnprocessedItems[table] || [];
              response.data.UnprocessedItems[table].forEach(function(res) {
                result.data.UnprocessedItems[table].push(res);
              });
            });
          }

          callback(null, result);
        }).send();
      })(req);
    }

    var q = queue(concurrency);

    requests.forEach(function(req) {
      q.defer(sendOne, req);
    });

    q.awaitAll(function(_, responses) {
      var err;
      var result = { };
      responses.forEach(function(res) {
        if (res.error) err = res.error;

        if (res.data.Responses) {
          result.Responses = result.Responses || {};
          Object.keys(res.data.Responses).forEach(function(table) {
            result.Responses[table] = result.Responses[table] || [];
            res.data.Responses[table].forEach(function(r) {
              result.Responses[table].push(r);
            });
          });
        }

        if (res.data.UnprocessedItems) {
          result.UnprocessedItems = result.UnprocessedItems || {};
          Object.keys(res.data.UnprocessedItems).forEach(function(table) {
            result.UnprocessedItems[table] = result.UnprocessedItems[table] || [];
            res.data.UnprocessedItems[table].forEach(function(r) {
              result.UnprocessedItems[table].push(r);
            });
          });
        }

        if (res.data.UnprocessedKeys) {
          result.UnprocessedKeys = result.UnprocessedKeys || {};
          Object.keys(res.data.UnprocessedKeys).forEach(function(table) {
            result.UnprocessedKeys[table] = result.UnprocessedKeys[table] || { Keys: [] };
            res.data.UnprocessedKeys[table].Keys.forEach(function(r) {
              result.UnprocessedKeys[table].Keys.push(r);
            });
          });
        }

        if (res.data.ConsumedCapacity) {
          result.ConsumedCapacity = result.ConsumedCapacity || {};
          reduceCapacity(result.ConsumedCapacity, res.data.ConsumedCapacity);
        }
      });

      callback(err, result);
    });
  }

  requests.batchWriteItemRequests = function(params) {
    var maxSize = 16 * 1024 * 1024;

    var paramSet = Object.keys(params.RequestItems).reduce(function(paramSet, tableName) {
      var toMake = [].concat(params.RequestItems[tableName]);

      return chop(toMake);

      function chop(requestsToMake) {
        // request set that we're building
        var requests = paramSet[paramSet.length - 1].RequestItems;
        requests[tableName] = requests[tableName] || [];

        // count existing requests in the current params
        var count = Object.keys(requests).reduce(function(count, tableName) {
          count += requests[tableName].length;
          return count;
        }, 0);

        // find existing requests size
        var size = Object.keys(requests).reduce(function(size, tableName) {
          size += requests[tableName].reduce(function(size, request) {
            size += request.PutRequest ? itemSize(request.PutRequest.Item) : 0;
            return size;
          }, 0);
          return size;
        }, 0);

        // Add requests one by one until it would put us over the size limit
        for (var i = 0; i < (25 - count); i++) {
          var next = requestsToMake[0];
          if (!next) return paramSet;
          var nextSize = next.PutRequest ? itemSize(next.PutRequest.Item) : 0;
          if (size + nextSize > maxSize) break;
          size += nextSize;
          requests[tableName].push(requestsToMake.shift());
        }

        // if there are no requests left, return the modified paramSet
        if (!requestsToMake.length) return paramSet;

        // otherwise start a new request set
        paramSet.push({ RequestItems: {}, ReturnConsumedCapacity: params.ReturnConsumedCapacity });
        return chop(requestsToMake);
      }
    }, [{ RequestItems: {}, ReturnConsumedCapacity: params.ReturnConsumedCapacity }]);

    var results = paramSet.map(function(params) {
      return client.batchWrite(params);
    });

    results.sendAll = sendAll.bind(null, results, 'batchWrite');
    return results;
  };

  requests.batchGetItemRequests = function(params) {
    var paramSet = Object.keys(params.RequestItems).reduce(function(paramSet, tableName) {
      var toMake = [].concat(params.RequestItems[tableName].Keys);
      return chop(toMake);

      function chop(requestsToMake) {
        // request set that we're building
        var requests = paramSet[paramSet.length - 1].RequestItems;
        requests[tableName] = requests[tableName] || { Keys: [] };

        // count existing requests in the current params
        var count = Object.keys(requests).reduce(function(count, tableName) {
          count += requests[tableName].Keys.length;
          return count;
        }, 0);

        // gather more from the requested params
        var more = requestsToMake.splice(0, 100 - count);

        // add them to the request set
        requests[tableName].Keys = requests[tableName].Keys.concat(more);

        // if there are no requests left, return the modified paramSet
        if (!requestsToMake.length) return paramSet;

        // otherwise start a new request set
        paramSet.push({ RequestItems: {}, ReturnConsumedCapacity: params.ReturnConsumedCapacity });
        return chop(requestsToMake);
      }
    }, [{ RequestItems: {}, ReturnConsumedCapacity: params.ReturnConsumedCapacity }]);

    var results = paramSet.map(function(params) {
      return client.batchGet(params);
    });

    results.sendAll = sendAll.bind(null, results, 'batchGet');
    return results;
  };

  requests.batchWriteAll = function(params, maxRetries) {
    maxRetries = maxRetries || 10;
    var requestSet = requests.batchWriteItemRequests(params);
    requestSet.sendAll = sendCompletely.bind(null, requestSet, 'batchWrite', maxRetries);
    return requestSet;
  };

  requests.batchGetAll = function(params, maxRetries) {
    maxRetries = maxRetries || 10;
    var requestSet = requests.batchGetItemRequests(params);
    requestSet.sendAll = sendCompletely.bind(null, requestSet, 'batchGet', maxRetries);
    return requestSet;
  };

  return requests;
};

function itemSize(item, skipAttr) {
  item = Object.keys(item).reduce(function(obj, key) {
    obj[key] = converter.input(item[key]);
    return obj;
  }, {});

  var size = 0;
  var attr;
  var type;
  var val;

  function bufferSize(sum, x) {
    return sum + new Buffer.from(x, 'base64').length;
  }

  function numberSetSize(sum, x) {
    x = big(x);
    return sum + Math.ceil(x.c.length / 2) + (x.e % 2 ? 1 : 2);
  }

  for (attr in item) {
    type = Object.keys(item[attr])[0];
    val = item[attr][type];
    size += skipAttr ? 2 : attr.length;
    switch (type) {
    case 'S':
      size += val.length;
      break;
    case 'B':
      size += new Buffer.from(val, 'base64').length;
      break;
    case 'N':
      val = big(val);
      size += Math.ceil(val.c.length / 2) + (val.e % 2 ? 1 : 2);
      break;
    case 'SS':
      size += val.reduce(function(sum, x) {
        return sum + x.length;
      }, skipAttr ? val.length : 0);
      break;
    case 'BS':
      size += val.reduce(bufferSize, skipAttr ? val.length : 0);
      break;
    case 'NS':
      size += val.reduce(numberSetSize, skipAttr ? val.length : 0);
      break;
    }
  }
  return size;
}
