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

    return dynamoRequest('getItem', params, function(err, resp) {
        if (err) return cb(err);
        cb(null, { Item: _(resp.items).first() });
    });
};

items.putItem = function(doc, opts, cb) {
    if(!cb) { cb = opts; opts ={};}
    var item = types.toDynamoTypes(doc);
    var params = {
        TableName: opts.table || config.table,
        Item: item
    };
    if(opts.expected) params.Expected = types.conditions(opts.expected);
    if(opts.conditionalOperator) params.ConditionalOperator = opts.conditionalOperator;

    return dynamoRequest('putItem', params, function(err, resp) {
        if (err) return cb(err);
        cb(null, {});
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
    if(opts.expected) params.Expected = types.conditions(opts.expected);
    if(opts.conditionalOperator) params.ConditionalOperator = opts.conditionalOperator;

    return dynamoRequest('updateItem', params, function(err, resp) {
        if (err) return cb(err);
        cb(null, {});
    });
};
