var util = require('util');
var _ = require('underscore');
var items = module.exports = {};
var types = require('./types');
var config = require('./config')();

items.getItem = function(key, opts, cb){
    if(!cb){ cb = opts; opts = {}; }
    key = types.toDynamoTypes(key);
    var params = {
        Key: key,
        TableName: opts.table || config.table,
        ConsistentRead: opts.consistentRead || false
    };
    config.dynamo.getItem(params, function(err, resp) {
        if (err) return cb(err);
        resp.Item = types.typesFromDynamo(resp.Item)
        if(resp && resp.Item && resp.Item.length === 1)
            resp.Item = resp.Item[0];
        cb(err, resp);
    });
};

items.putItem = function(doc, opts, cb) {
    if(!cb) { cb = opts; opts ={};}
    var item = types.toDynamoTypes(doc);
    var params = {
        TableName: opts.table || config.table,
        Item: item
    };
    config.dynamo.putItem(params, cb);
};

items.updateItem = function(key, doc, opts, cb) {
    if(!cb) { cb = opts; opts ={};}
    var attrs = types.toAttributeUpdates(doc);

    var params = {
        TableName: opts.table || config.table,
        Key: types.toDynamoTypes(key),
        AttributeUpdates: attrs
    };
    if(opts.expected) params.Expected = opts.expected;
    if(opts.conditionalOperator) params.ConditionalOperator = opts.conditionalOperator;

    config.dynamo.updateItem(params, cb);
};
