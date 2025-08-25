var queue = require('queue-async');
const { DescribeTableCommand, CreateTableCommand, DeleteTableCommand } = require('@aws-sdk/client-dynamodb');

function activeCheck(client, params, callback) {
  const describeCommand = new DescribeTableCommand({ TableName: params.TableName });
  
  client.send(describeCommand)
    .then(data => {
      if (data.Table.TableStatus !== 'ACTIVE')
        return setTimeout(activeCheck, 1000, client, params, callback);
      callback(null, { TableDefinition: data.Table });
    })
    .catch(err => {
      if (err.name === 'ResourceNotFoundException') {
        const createCommand = new CreateTableCommand(params);
        return client.send(createCommand)
          .then(() => setTimeout(activeCheck, 0, client, params, callback))
          .catch(createErr => callback(createErr));
      }
      callback(err);
    });
}

function deletedCheck(client, params, callback) {
  const describeCommand = new DescribeTableCommand({ TableName: params.TableName });
  
  client.send(describeCommand)
    .then(data => {
      if (data.Table.TableStatus === 'ACTIVE') {
        const deleteCommand = new DeleteTableCommand({ TableName: params.TableName });
        return client.send(deleteCommand)
          .then(() => setTimeout(deletedCheck, 0, client, params, callback))
          .catch(deleteErr => callback(deleteErr));
      }
      return setTimeout(deletedCheck, 1000, client, params, callback);
    })
    .catch(err => {
      if (err.name === 'ResourceNotFoundException') return callback();
      callback(err);
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
  };

  return table;
};
