var util = require('util');
var queue = require('queue-async');
var _ = require('underscore');
var items = module.exports = {};
var types = require('./types');
var big = require('big.js');
var config = require('./config')();

items.putItems = function(items, opts, cb) {
    if (!cb) {
        cb = opts;
        opts = {};
    }

    var table = opts.table || config.table;
    var params = {
        RequestItems: {}
    };
    var padding = JSON.stringify(params).length;
    var maxSize = 1024 * 1024;
    var q = queue(10);

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
            thisChunk = 0;
        }
    }

    if (chunk.length) chunks.push(chunk);

    chunks.forEach(function(putRequests) {
        var p = {
            RequestItems: {}
        };
        p.RequestItems[table] = putRequests;
        q.defer(config.dynamo.batchWriteItem.bind(config.dynamo), p);
    });

    q.awaitAll(function(err, res) {
        cb(err, res);
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