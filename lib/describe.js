var _ = require('underscore');

module.exports = function(config) {
    var dynamoRequest = require('./dynamoRequest')(config);

    return {
        describeTable: function(opts, cb) {
            if (!cb && _.isFunction(opts)) {
                cb = opts;
                opts = {};
            }
            if (!opts) opts = {};

            var params = {
                TableName: opts.table || config.table
            };

            return dynamoRequest('describeTable', params, opts, cb);
        }
    };
};
