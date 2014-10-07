var config = require('./config')();
var types = require('./types');
var dynamoRequest = require('./dynamoRequest');
var _ = require('underscore');

module.exports.scan = function(opts, cb) {
    if(!cb && _.isFunction(opts)) { cb = opts; opts = {}; }
    if(!opts) opts = {};

    var params = {
        TableName: opts.table || config.table
    };

    if (opts.attributes) {
        params.AttributesToGet = opts.attributes;
        params.Select = 'SPECIFIC_ATTRIBUTES';
    } else  {
        params.Select = opts.select || 'ALL_ATTRIBUTES';
    }
    if (opts.limit) {
        params.Limit = opts.limit;
    }
    if (opts.capacity) params.ReturnConsumedCapacity = opts.capacity;

    return dynamoRequest('scan', params, opts, cb);
};
