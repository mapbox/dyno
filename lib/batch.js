var util = require('util');
var queue = require('queue-async');
var _ = require('underscore');
var items = module.exports = {};
var types = require('./types');
var config = require('./config')();

items.putItems = function(items, opts, cb) {
    if(!cb) { cb = opts; opts = {}; }

    var table = opts.table || config.table;
    var params = { RequestItems: {} };
    var padding = JSON.stringify(params).length;
    var maxSize = 1024 * 1024;
    var q = queue(10);

    var putRequests = items.map(function(item) {
        return { PutRequest: { Item: types.toDynamoTypes(item) } };
    });

    var putRequestSizes = putRequests.map(function(p) {
        return JSON.stringify(p).length + padding;
    });

    if (!putRequestSizes.every(function(pr) {
        return pr < maxSize;
    })) {
        return callback(new Error('a chunk in this putItems call is too large.'));
    }

    var chunks = [], chunk = [], thisChunk = 0;

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
        var p = { RequestItems: {} };
        p.RequestItems[table] = putRequests;
        q.defer(config.dynamo.batchWriteItem.bind(config.dynamo),
                p, cb);
    });

    q.awaitAll(function(err, res) {
        callback(err, res);
    });
};
