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

    if(opts.expected) params.Expected = opts.expected;
    if(opts.operator) params.ConditionalOperator = operator;
    if(opts.capacity) params.ReturnConsumedCapacity = capacity;
    if(opts.metrics) params.ReturnItemCollectionMetrics = metrics;
    if(opts.values) params.ReturnValues = values;

    config.dynamo.putItem(params, items._response(config.dynamo.putItem, params, opts, cb));
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
    if(opts.operator) params.ConditionalOperator = operator;
    if(opts.capacity) params.ReturnConsumedCapacity = capacity;
    if(opts.metrics) params.ReturnItemCollectionMetrics = metrics;
    if(opts.values) params.ReturnValues = values;

    config.dynamo.updateItem(params, cb);
};

items.deleteItem = function(key, opts, cb) {
    if(!cb) { cb = opts; opts ={};}

    var params = {
        TableName: opts.table || config.table,
        Key: types.toDynamoTypes(key)
    };
    
    if(opts.expected) params.Expected = opts.expected;
    if(opts.operator) params.ConditionalOperator = operator;
    if(opts.capacity) params.ReturnConsumedCapacity = capacity;
    if(opts.metrics) params.ReturnItemCollectionMetrics = metrics;
    if(opts.values) params.ReturnValues = values;

    config.dynamo.deleteItem(params, cb);
};

items._response = function(func, params, opts, cb) {
    var attempts = 0;

    var retry = function() {
        func.bind(config.dynamo)(params, handleResp);
    };

    var handleResp  = function(err, resp) {
        if(!opts.errors || !err) {
            return cb(err, resp);
        }

        if (err.code = "ProvisionedThroughputExceededException") {
            if(_.isNumber(opts.errors.throughput) && attempts <= opts.errors.throughput){
                attempts++;
                setTimeout(retry, 100 * attempts * attempts);
            } else {
                cb(err, resp);
            }
        }
    }
    return handleResp;
};
