var config = require('./config')();
var types = require('./types');
var stream = require('stream');
var _ = require('underscore');

var handledRequests = [
    'query',
    'scan',
    'updateItem',
    'putItem',
    'getItem',
    'batchGetItem',
    'batchWriteItem'
];

module.exports = function(type, parameters, opts, callback) {
    if (typeof opts === 'function') {
        callback = opts;
        opts = {};
    }

    if (handledRequests.indexOf(type) === -1) {
        if (callback) callback(new Error('Invalid request type'));
        return;
    }

    var items = [];
    var metas = [];
    var read = false;

    var readable = new stream.Readable({objectMode:true});
    readable._read = function(size) {
        read = true;
        while (items.length > 0) {
            readable.push(items.shift());
        }
    };

    // 24 attempts is a hard cut-off at about 1 min delay between retries
    var maxAttempts = _.isNumber(opts.throughputAttempts) ?
        Math.min(opts.throughputAttempts, 24) : 24;

    var params = _(parameters).clone();
    var page = 0;

    if (opts.pages === undefined && callback) opts.pages = 1;
    if (!opts.pages) opts.pages = Infinity;

    request();

    function request(start) {
        var attempts = 0;
        if (start) params.ExclusiveStartKey = start;

        config.dynamo[type](params, response);

        function response(err, resp) {
            attempts++;

            function throttledRetry(requestParams) {
                if (attempts < maxAttempts) {
                    return setTimeout(function() {
                        config.dynamo[type](requestParams, response);
                    }, 100 * attempts * attempts);
                } else {
                    err = err || new Error('Could not complete the batch request');
                    err.attempts = attempts;
                    if (unprocessed) err.unprocessed = unprocessed;
                    if (callback) return callback(err);
                    else return readable.emit('error', err);
                }
            }

            if (err && err.code === 'ProvisionedThroughputExceededException')
                return throttledRetry(params);

            if (err && !callback) return readable.emit('error', err);
            if (err) return callback(err);

            var unprocessed = resp.UnprocessedKeys || resp.UnprocessedItems;
            if (unprocessed && Object.keys(unprocessed).length > 0) {
                var newParams = _({}).extend(params, { RequestItems: unprocessed });
                return throttledRetry(newParams);
            }

            if (resp.Item) resp.Items = [ resp.Item ];
            if (resp.Attributes) resp.Items = [ resp.Attributes ];
            if (resp.Responses && resp.Responses[config.table])
                resp.Items = resp.Responses[config.table];

            resp.Items = resp.Items ? types.typesFromDynamo(resp.Items) : [];
            items = items.concat(resp.Items);

            var meta = {};
            if (resp.ConsumedCapacity) meta.capacity = resp.ConsumedCapacity;
            metas.push(meta);

            while (read && items.length > 0) {
                readable.push(items.shift());
            }

            page++;
            if (page < opts.pages && resp.LastEvaluatedKey)
                return request(resp.LastEvaluatedKey);

            if (read) readable.push(null);

            if (callback) callback(null, items, metas);
        }
    }

    return readable;
};
