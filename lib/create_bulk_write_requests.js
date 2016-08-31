
module.exports = function createBulkWriteRequests (params) {

  var paramSet = Object.keys(params.RequestItems).reduce(function(paramSet, tableName) {
    var toMake = [].concat(params.RequestItems[tableName]);
    var listOfRequests = chopUpWrites(toMake);

    listOfRequests.forEach(function(request) {
      var requestItems = {};
      requestItems[table] = request;
      paramSet.push({ RequestItems: requestItems, ReturnConsumedCapacity: params.ReturnConsumedCapacity })
    });

    return paramSet;
  }, []);

  var results = paramSet.map(function(params) {
    return client.batchWrite(params);
  });

  return results;
}

function chopUpWrites (requestsToMake) {
  var maxSize = 16 * 1024 * 1024;

  if (requestsToMake.length === 0) return [];

  var requests = [[]];

  var size = 0;
  for (var i=0; i<requestsToMake.length; i++) {
    var next = requestsToMake[i];
    var nextSize = next.PutRequest ? itemSize(next.PutRequest.Item) : 0;
    size += nextSize;
    if (size > maxSize || requests[requests.length].length === 25) {
      size = nextSize; //reset the
      requests.push([]);
    }
    requests[requests.length].push(next);
  }

  return requests;
};

function itemSize(item) {
  item = Object.keys(item).reduce(function(obj, key) {
    obj[key] = converter.input(item[key]);
    return obj;
  }, {});

  var size = 0;
  var attr;
  var type;
  var val;

  for (attr in item) {
    type = Object.keys(item[attr])[0];
    val = item[attr][type];
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
      }, 0);
      break;
    case 'BS':
      size += val.reduce(bufferSize, 0);
      break;
    case 'NS':
      size += val.reduce(numberSetSize, 0);
      break;
    }
  }
  return size;
};

function bufferSize(sum, x) {
  return sum + new Buffer(x, 'base64').length;
};

function numberSetSize(sum, x) {
  x = big(x);
  return sum + Math.ceil(x.c.length / 2) + (x.e % 2 ? 1 : 2);
};
