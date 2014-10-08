var util = require('util');
var queue = require('queue-async');
var _ = require('underscore');
var items = module.exports = {};
var types = require('./types');
var big = require('big.js');
var config = require('./config')();
var dynamoRequest = require('./dynamoRequest');

items.getItems = function(keys, opts, cb) {
    if (typeof opts === 'function') {
        cb = opts;
        opts = {};
    }

    var table = opts.table || config.table;
    var params = { RequestItems: {} };
    var requests = params.RequestItems[table] = {};

    params.ReturnConsumedCapacity = opts.capacity || 'NONE';
    if (opts.attributes) requests.Attributes = opts.attributes;
    if (opts.consistent) requests.ConsistentRead = true;

    keys = keys.map(function(key) {
        return types.toDynamoTypes(key);
    });

    var chunks = [];
    for (var c = 0; c < Math.ceil(keys.length / 100); c++) {
        chunks[c] = keys.slice(100 * c, 100 * (c + 1));
    }

    var q = queue(10);
    var metas = [];
    chunks.forEach(function(keys) {
        requests.Keys = keys;
        q.defer(function(callback) {
            dynamoRequest('batchGetItem', params, opts, function(err, items, meta) {
                if (err) return callback(err);
                metas.push(meta);
                callback(null, items);
            });
        });
    });
    q.awaitAll(function(err, items) {
        if (err) return cb(err);
        cb(null, _(items).flatten(), _(metas).flatten());
    });
};

items.putItems = function(items, opts, cb) {
    if (!cb) {
        cb = opts;
        opts = {};
    }

    var table = opts.table || config.table;
    var maxSize = 1024 * 1024;
    var q = queue(10);

    var params = {};
    if (opts.capacity) params.ReturnConsumedCapacity = opts.capacity;

    var putRequests = items.map(function(item) {
        return {
            PutRequest: {
                Item: types.toDynamoTypes(item)
            }
        };
    });

    var putRequestSizes = putRequests.map(function(p) {
        return itemSize(p.PutRequest.Item);
    });

    if (!putRequestSizes.every(function(pr) {
        return pr < maxSize;
    })) {
        return cb(new Error('a chunk in this putItems call is too large.'));
    }

    var chunks = [],
        chunk = [],
        thisChunk = 0;

    for (var i = 0; i < putRequestSizes.length; i++) {
        if ((thisChunk + putRequestSizes[i]) < maxSize &&
            chunk.length < 25) {
            chunk.push(putRequests[i]);
            thisChunk += putRequestSizes[i];
        } else {
            chunks.push(chunk);
            chunk = [];
            chunk.push(putRequests[i]);
            thisChunk = putRequestSizes[i];
        }
    }

    if (chunk.length) chunks.push(chunk);

    chunks.forEach(function(putRequests) {
        var p = _({}).extend(params, {
            RequestItems: {}
        });
        p.RequestItems[table] = putRequests;
        q.defer(function(callback) {
            dynamoRequest('batchWriteItem', p, function(err, res, meta) {
                if (err) return callback(err);
                callback(null, meta);
            });
        });
    });

    q.awaitAll(function(err, metas) {
        if (err) return cb(err);
        cb(null, null, _(metas).flatten());
    });
};

items.deleteItems = function(itemKeys, opts, cb) {
    if (!cb) {
        cb = opts;
        opts = {};
    }
    var table = opts.table || config.table;
    var q = queue(10);

    var params = {};
    if (opts.capacity) params.ReturnConsumedCapacity = opts.capacity;

    var deleteRequests = itemKeys.map(function(key) {
        return {
            DeleteRequest: {
                Key: types.toDynamoTypes(key)
            }
        };
    });

    var chunks = [],
        chunk = [];

    for (var i = 0; i < deleteRequests.length; i++) {
        if (chunk.length < 25) {
            chunk.push(deleteRequests[i]);
        } else {
            chunks.push(chunk);
            chunk = [];
            chunk.push(deleteRequests[i]);
        }
    }

    if (chunk.length) chunks.push(chunk);

    chunks.forEach(function(deleteRequests) {
        var d = _({}).extend(params, {
            RequestItems: {}
        });
        d.RequestItems[table] = deleteRequests;
        q.defer(function(callback) {
            dynamoRequest('batchWriteItem', d, function(err, res, meta) {
                if (err) return callback(err);
                callback(null, meta);
            });
        });
    });

    q.awaitAll(function(err, metas) {
        if (err) return cb(err);
        cb(null, null, _(metas).flatten());
    });
};

function itemSize(item, skipAttr) {
    var size = 0,
        attr, type, val;
    for (attr in item) {
        type = Object.keys(item[attr])[0];
        val = item[attr][type];
        size += skipAttr ? 2 : attr.length;
        switch (type) {
            case 'S':
                size += val.length;
                break;
            case 'B':
                size += new Buffer(val, 'base64').length;
                break;
            case 'N':
                val = big(val);
                size += Math.ceil(val.c.length / 2) + (val.e % 2 ? 1 : 2);
                break;
            case 'SS':
                size += val.reduce(function(sum, x) {
                    return sum + x.length;
                }, skipAttr ? val.length : 0);
                break;
            case 'BS':
                size += val.reduce(function(sum, x) {
                    return sum + new Buffer(x, 'base64').length;
                }, skipAttr ? val.length : 0);
                break;
            case 'NS':
                size += val.reduce(function(sum, x) {
                    x = big(x);
                    return sum + Math.ceil(x.c.length / 2) + (x.e % 2 ? 1 : 2);
                }, skipAttr ? val.length : 0);
                break;
        }
    }
    return size;
}
