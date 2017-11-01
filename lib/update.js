module.exports = updates;

function updates(client) {
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

    updateParams = Object.assign(updateParams, expressionObject);
    return client.update(updateParams, callback);
  };

  return updates;
}