var config = require('./config')();
var types = require('./types');
var stream = require('stream');
var _ = require('underscore');

var handledRequests = [
    'query',
    'scan',
    'updateItem',
    'putItem',
    'getItem'
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
    var read = false;

    var readable = new stream.Readable({objectMode:true});
    readable._read = function(size) {
        read = true;
        while (items.length > 0) {
            readable.push(items.shift());
        }
    };

    var maxAttempts = _.isNumber(opts.throughputAttempts) ? opts.throughputAttempts : 1;
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

            if (err && err.code === 'ProvisionedThroughputExceededException') {
                if (attempts < maxAttempts) {
                    return setTimeout(function() {
                        config.dynamo[type](params, response);
                    }, 100 * attempts * attempts);
                } else {
                    if (callback) return callback(err);
                    else return readable.emit('error', err);
                }
            }

            if (err && !callback) return readable.emit('error', err);
            if (err) return callback(err);

            if (resp.Item) resp.Items = [ resp.Item ];
            resp.Items = resp.Items ? types.typesFromDynamo(resp.Items) : [];
            resp.Items.forEach(function(item) {
                items.push(item);
            });

            while (read && items.length > 0) {
                readable.push(items.shift());
            }

            page++;
            if (page < opts.pages && resp.LastEvaluatedKey)
                return request(resp.LastEvaluatedKey);

            if (read) readable.push(null);

            var result = { count: resp.Count, items: items };
            if (resp.ConsumedCapacity) result.capacity = resp.ConsumedCapacity;

            if (callback) callback(null, result);
        }
    }

    return readable;
};
