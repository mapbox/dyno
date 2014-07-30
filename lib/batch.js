var util = require('util');
var _ = require('underscore');
var items = module.exports = {};
var types = require('./types');
var config = require('./config')();

items.putItems = function(items, opts, cb) {
    if(!cb) { cb = opts; opts = {}; }
    var table = opts.table || config.table;
    var params = { RequestItems: {} };
    params.RequestItems[table] = items.map(function(item) {
        return {
            PutRequest: {
                Item: types.toDynamoTypes(item)
            }
        };
    });
    config.dynamo.batchWriteItem(params, cb);
};
