var util = require('util');
var _ = require('underscore');
var items = module.exports = {};
var types = require('./types');
var config = require('./config')();
var dynamoRequest = require('./dynamoRequest');

items.getItem = function(key, opts, cb){
    if(!cb){ cb = opts; opts = {}; }
    key = types.toDynamoTypes(key);
    var params = {
        Key: key,
        TableName: opts.table || config.table,
        ConsistentRead: opts.consistentRead || false
    };

    if (opts.capacity) params.ReturnConsumedCapacity = 'INDEXES';

    return dynamoRequest('getItem', params, opts, function(err, resp) {
        if (err) return cb(err);
        cb(null, { Item: _(resp.items).first() }, resp.capacity);
    });
};

items.putItem = function(doc, opts, cb) {
    if(!cb) { cb = opts; opts ={};}
    var item = types.toDynamoTypes(doc);
    var params = {
        TableName: opts.table || config.table,
        Item: item
    };

    if (opts.expected) params.Expected = types.conditions(opts.expected);
    if (opts.conditionalOperator) params.ConditionalOperator = opts.conditionalOperator;
    if (opts.capacity) params.ReturnConsumedCapacity = 'INDEXES';

    return dynamoRequest('putItem', params, opts, function(err, resp) {
        if (err) return cb(err);
        cb(null, {}, resp.capacity);
    });
};

items.updateItem = function(key, doc, opts, cb) {
    if(!cb) { cb = opts; opts ={};}
    var attrs = types.toAttributeUpdates(doc);
    var params = {
        TableName: opts.table || config.table,
        Key: types.toDynamoTypes(key),
        AttributeUpdates: attrs
    };

    if (opts.expected) params.Expected = types.conditions(opts.expected);
    if (opts.conditionalOperator) params.ConditionalOperator = opts.conditionalOperator;
    if (opts.capacity) params.ReturnConsumedCapacity = 'INDEXES';

    return dynamoRequest('updateItem', params, opts, function(err, resp) {
        if (err) return cb(err);
        cb(null, {}, resp.capacity);
    });
};
