module.exports = function(params) {
    var paramSet = Object.keys(params.RequestItems).reduce(function(paramSet, tableName) {
      var reads = chopUpReads(params.RequestItems[tableName].Keys);
      for (var i=0; reads.length; i++) {
        var param = { RequestItems: {}, ReturnConsumedCapacity: params.ReturnConsumedCapacity };
        param[tableName] = {
          Keys: reads[i]
        };
        paramSet.push(param);
      }

      return paramSet;
    }, []);

    var results = paramSet.map(function(params) {
      return client.batchGet(params);
    });

    results.sendAll = sendAll.bind(null, results, 'batchGet');
    return results;
  };

  function chopUpReads (tableName, keysToGet) {
    if (keysToGet.length === 0) return [];

    var keys = [[]];
    for (var i=0; i<keysToGet.length; i++) {
      if (setOfKeys[setOfKeys.length-1].length === 100) {
        setOfKeys.push([]);
      }
      setOfKeys[setOfKeys.length-1].push(keysToGet[i]);
    }

    return setOfKeys;

    return setOfKeys.map(function(request) {
      var out =
      out.RequestItems[tableName] = {
        Keys: request
      }
      return out;
    })
  }
