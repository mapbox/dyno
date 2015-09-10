var types = require('./types');
var _ = require('underscore');
var stream = require('stream');

module.exports = function(config) {
    var dynamoRequest = require('./dynamoRequest')(config);
    var batch = require('./batch')(config);

    var items = {
        /**
         * Query items from item from a table
         * @param {String} conditions - criteria for the query
         * @param {Object} [opts]
         * @param {Object} [opts.index] - Name of the DynamoDB index to use for the query
         * @param {Object} [opts.start]
         * @param {Object} [opts.ascending]
         * @param {String} [opts.table] - table to use for the query if provided; otherwise uses the value from Dyno client config
         * @param {Object} [opts.filter]
         * @param {Object} [opts.attributes]
         * @param {Object} [opts.select]
         * @param {Boolean} [opts.capacity] - indicates that the DynamoDB response should include the capacity consumed by the update operation
         * @param {Function} cb callback
         */
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
        },

        queryBatchGet: function(index, hashKey, conditions, opts, cb) {
            if (!cb && _.isFunction(opts)) {
                cb = opts;
                opts = {};
            }
            if (!opts) opts = {};

            var readOpts = _({ index: index }).defaults(opts);
            var gsiQuery = items.query(conditions, readOpts);

            var batchGet = new stream.Transform({ objectMode: true });

            batchGet.queue = [];

            batchGet.fetch = function(keys, callback) {
                batch.getItems(keys, opts, function(err, items) {
                    if (err) return callback(err);
                    batchGet.queue = [];
                    items.forEach(function(item) {
                        batchGet.push(item);
                    });
                    callback();
                });
            };

            batchGet._transform = function(indexItem, enc, callback) {
                var key = {};
                key[hashKey] = indexItem[hashKey];

                batchGet.queue.push(key);
                if (batchGet.queue.length < 100) return callback();
                batchGet.fetch(batchGet.queue, callback);
            };

            batchGet._flush = function(callback) {
                if (!batchGet.queue.length) return callback();
                batchGet.fetch(batchGet.queue, callback);
            };

            if (cb) {
                var results = [];

                batchGet
                    .once('error', cb)
                    .on('data', function(item) { results.push(item); })
                    .on('end', function() {
                        cb(null, results);
                    })
                    .resume();
            }

            return gsiQuery.pipe(batchGet);
        }
    };

    return items;
};
