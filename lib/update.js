var _ = require('underscore');

module.exports = function(client) {
  var updates = {};

  updates.dynamicUpdate = function (newObject, updateParams, callback) {
    var expressionObject = {
      ExpressionAttributeNames: {},
      ExpressionAttributeValues: {}
    };
    var updateExpressionParts = [];

    Object.keys(newObject).forEach(function (key) {
      if (Object.keys(updateParams.Key).indexOf(key) !== -1) return;

      expressionObject.ExpressionAttributeNames['#' + key] = key;
      expressionObject.ExpressionAttributeValues[':' + key] = newObject[key];
      updateExpressionParts.push('#' + key + ' = :' + key);
    });
    expressionObject.UpdateExpression = 'set ' + updateExpressionParts.join(', ');

    updateParams = _.extend(updateParams, expressionObject);
    return client.update(updateParams, callback);
  };

  return updates;
};