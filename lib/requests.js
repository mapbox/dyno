var big = require('big.js');
var converter = require('aws-sdk/lib/dynamodb/converter');
var queue = require('queue-async');
var _ = require('underscore');

module.exports = function(client) {
  var requests = {};

  function sendAll(requests, concurrency, callback) {
    if (typeof concurrency === 'function') {
      callback = concurrency;
      concurrency = 1;
    }

    var q = queue(concurrency);

    requests.forEach(function(req) {
      q.defer(function(next) {
        req.on('complete', function(response) {
          next(null, response);
        }).send();
      });
    });

    q.awaitAll(function(err, responses) {
      if (err) return callback(err);

      var errors = [];
      var data = [];
      responses.forEach(function(response) {
        errors.push(response.error);
        data.push(response.data);
      });

      var unprocessed = data.reduce(function(unprocessed, data) {
        if (data.UnprocessedItems) {
          _(data.UnprocessedItems).each(function(items, table) {
            unprocessed.RequestItems[table] = unprocessed.RequestItems[table] ?
              unprocessed.RequestItems[table].concat(items) : items;
          });
        }
        return unprocessed;
      }, { RequestItems: {} });

      unprocessed = Object.keys(unprocessed.RequestItems).length ?
        requests.batchWriteItemRequests(unprocessed) : undefined;

      errors = errors.reduce(function(errors, err) {
        if (err) errors = errors ? errors.concat([err]) : [err];
        return errors;
      });

      callback(errors, data, unprocessed);
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
        paramSet.push({ RequestItems: {} });
        return chop(requestsToMake);
      }
    }, [{ RequestItems: {} }]);

    var results = paramSet.map(function(params) {
      return client.batchWrite(params);
    });

    results.sendAll = sendAll.bind(null, results);
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
        paramSet.push({ RequestItems: {} });
        return chop(requestsToMake);
      }
    }, [{ RequestItems: {} }]);

    var results = paramSet.map(function(params) {
      return client.batchGet(params);
    });

    results.sendAll = sendAll.bind(null, results);
    return results;
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
    return sum + new Buffer(x, 'base64').length;
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
      size += new Buffer(val, 'base64').length;
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
