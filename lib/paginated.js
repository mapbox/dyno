var stream = require('./stream');
var _ = require('underscore');

module.exports = function(client) {
  var paginated = {};

  function read(type, params, callback) {
    if (typeof params === 'function') {
      callback = params;
      params = {};
    }

    if (params.Pages === undefined || params.Pages === null)
      return client[type](_(params).omit('Pages'), callback);

    var items = [];
    var readable = stream(client)[type](_(params).omit('Pages'), { pages: params.Pages })
      .on('validate', function() {
        if (params.Pages === 0) throw new Error('Pages must be an integer greater than 0');
      })
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

  paginated.query = function(params, callback) {
    return read('query', params, callback);
  };

  paginated.scan = function(params, callback) {
    return read('scan', params, callback);
  };

  return paginated;
};
