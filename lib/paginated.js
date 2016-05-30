var stream = require('./stream');
var _ = require('underscore');

module.exports = function(client) {
  var paginated = {};

  function read(type, params, callback) {
    if (typeof params === 'function') {
      callback = params;
      params = {};
    }

    if (params.hasOwnProperty('Pages')) {
      if (params.Pages === 0) return callback(null, {
        Items: [],
        Count: 0,
        ScannedCount: 0
      });

      if (params.Pages) {
        var items = [];
        var readable = stream(client)[type](_(params).omit('Pages'), { pages: params.Pages })
          .on('error', callback)
          .on('data', function(item) { items.push(item); })
          .on('end', function() {
            callback(null, {
              Items: items,
              Count: readable.Count,
              ScannedCount: readable.ScannedCount,
              LastEvaluatedKey: readable.LastEvaluatedKey,
              ConsumedCapacity: readable.ConsumedCapacity
            });
          });

        return readable;
      }
    }

    return client[type](_(params).omit('Pages'), callback);
  }

  paginated.query = function(params, callback) {
    return read('query', params, callback);
  };

  paginated.scan = function(params, callback) {
    return read('scan', params, callback);
  };

  return paginated;
};
