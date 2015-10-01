var queue = require('queue-async');

function activeCheck(client, params, callback) {
  client.describeTable({ TableName: params.TableName }, function(err, data) {
    if (err && err.code === 'ResourceNotFoundException') {
      return client.createTable(params, function(err) {
        if (err) return callback(err);
        setTimeout(activeCheck, 0, client, params, callback);
      });
    }
    if (err) return callback(err);
    if (data.Table.TableStatus !== 'ACTIVE')
      return setTimeout(activeCheck, 1000, client, params, callback);
    callback(null, { TableDefinition: data.Table });
  });
}

function deletedCheck(client, params, callback) {
  client.describeTable({ TableName: params.TableName }, function(err, data) {
    if (err && err.code === 'ResourceNotFoundException') return callback();
    if (err) return callback(err);
    if (data.Table.TableStatus === 'ACTIVE') {
      return client.deleteTable({ TableName: params.TableName }, function(err) {
        if (err) return callback(err);
        setTimeout(deletedCheck, 0, client, params, callback);
      });
    }
    return setTimeout(deletedCheck, 1000, client, params, callback);
  });
}

module.exports = function(client, secondClient) {
  var table = {};

  table.create = function(params, callback) {
    activeCheck(client, params, callback);
  };

  table.delete = function(params, callback) {
    deletedCheck(client, params, callback);
  };

  table.multiCreate = function(params, callback) {
    queue()
      .defer(activeCheck, client, params)
      .defer(activeCheck, secondClient, params)
      .awaitAll(function(err) { callback(err); });
  };

  table.multiDelete = function(callback) {
    queue()
      .defer(deletedCheck, client, {})
      .defer(deletedCheck, secondClient, {})
      .awaitAll(function(err) { callback(err); });
  }

  return table;
};
