var types = require('./types');
var stream = require('stream');
var _ = require('underscore');
var kinesisClient = require('./kinesis');

var handledRequests = [
    'query',
    'scan',
    'updateItem',
    'putItem',
    'getItem',
    'deleteItem',
    'batchGetItem',
    'batchWriteItem',
    'describeTable',
    'deleteItem'
];

var noItems = [
    'describeTable'
];

module.exports = function(config) {
    return function(type, parameters, opts, callback) {
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
        var streamMode = !callback;
        var readMore = true;
        var currentKey;

        var readable = new stream.Readable({objectMode:true});
        readable._read = function(size) {
            // make records readable until hit high water mark or runs out
            var status = true;
            while (status && items.length)
                status = readable.push(items.shift());

            // hit high water mark, subsequent calls will bring it below
            if (items.length) return;

            // if there's nothing more to request, end the stream
            if (!readMore) return readable.push(null);

            // if below high water mark, make a dynamodb request, but make sure
            // only one request is outstanding at a time
            if (status && !readable.pending) request(currentKey);
        };

        // 24 attempts is a hard cut-off at about 1 min delay between retries
        var throughputAttempts = _.isNumber(opts.throughputAttempts) ?
            Math.min(opts.throughputAttempts, 24) : _.isNumber(config.throughputAttempts) ?
            Math.min(config.throughputAttempts, 24) : 10;

        var batchAttempts = _.isNumber(opts.batchAttempts) ?
            Math.min(opts.batchAttempts, 24) : _.isNumber(config.batchAttempts) ?
            Math.min(config.batchAttempts, 24) : 10;

        var params = _(parameters).clone();
        var page = 0;

        if (opts.pages === undefined && callback) opts.pages = 1;
        if (!opts.pages) opts.pages = Infinity;

        if (!streamMode) request(opts.start);

        function request(start) {
            currentKey = false;
            var attempts = 0;
            if (start) params.ExclusiveStartKey = start;
            if (streamMode) readable.pending = true;

            config.dynamo[type](params, response)
                .on('retry', function(resp) {
                    if (resp.error && resp.error.code === 'ProvisionedThroughputExceededException') {
                        resp.maxRetries = throughputAttempts;
                        resp.error.retryDelay = 50 * Math.pow(2, resp.retryCount - 1);

                        if (resp.retryCount < throughputAttempts) resp.error.retryable = true;
                        else resp.error.retryable = false;
                    }
                });

            function response(err, resp) {
                if (streamMode) {
                    readable.pending = false;
                    readable.emit('dbrequest', params, resp);
                }
                attempts++;

                function throttledRetry(maxAttempts, requestParams) {
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

                if (err && !callback) return readable.emit('error', err);
                if (err) return callback(err);

                var unprocessed = resp.UnprocessedKeys || resp.UnprocessedItems;
                if (unprocessed && Object.keys(unprocessed).length > 0) {
                    var newParams = _({}).extend(params, { RequestItems: unprocessed });
                    return throttledRetry(batchAttempts, newParams);
                }

                if (resp.Item) resp.Items = [resp.Item];
                if (resp.Attributes) resp.Items = [resp.Attributes];
                if (resp.Responses && resp.Responses[config.table])
                    resp.Items = resp.Responses[config.table];

                if (type.indexOf(noItems) === -1) {
                    resp.Items = resp.Items ? types.typesFromDynamo(resp.Items) : [];
                    items = items.concat(resp.Items);
                } else {
                    items = resp;
                }

                var meta = {};
                if (resp.ConsumedCapacity) meta.capacity = resp.ConsumedCapacity;
                if (resp.LastEvaluatedKey) meta.last = resp.LastEvaluatedKey;
                metas.push(meta);

                page++;
                currentKey = resp.LastEvaluatedKey;
                readMore = !!currentKey && page < opts.pages;
                if (streamMode) return readable._read();
                else if (readMore) return request(currentKey);

                var kinesis = kinesisClient(type, config);
                if (!kinesis.enabled) return callback(null, items, metas);
                kinesis.put(params, function(err) {
                    if (err) return callback(err);
                    callback(null, items, metas);
                });
            }
        }

        return readable;
    };
};
