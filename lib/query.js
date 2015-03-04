var types = require('./types');
var _ = require('underscore');

module.exports = function(config) {
    var dynamoRequest = require('./dynamoRequest')(config);

    return {
        query: function(conditions, opts, cb) {
            if (!cb && _.isFunction(opts)) {
                cb = opts;
                opts = {};
            }
            if (!opts) opts = {};

            var keyConditions = types.conditions(conditions);
            var params = {
                TableName: opts.table || config.table,
                KeyConditions: keyConditions
            };
            if (opts.filter) {
                params.QueryFilter = types.conditions(opts.filter);
            }
            if (opts.index) {
                params.IndexName = opts.index;
            }
            if (opts.limit) {
                params.Limit = opts.limit;
            }
            if (opts.start) {
                params.ExclusiveStartKey = opts.start;
            }
            if (opts.attributes) {
                params.AttributesToGet = opts.attributes;
                params.Select = 'SPECIFIC_ATTRIBUTES';
            } else {
                params.Select = opts.select || (opts.index ? 'ALL_PROJECTED_ATTRIBUTES' : 'ALL_ATTRIBUTES');
            }
            if (opts.capacity) params.ReturnConsumedCapacity = opts.capacity;
            if (opts.ascending === false) {
                params.ScanIndexForward = false;
            }

            return dynamoRequest('query', params, opts, cb);
        }
    };
};
