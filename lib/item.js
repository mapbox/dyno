var _ = require('underscore');
var types = require('./types');

module.exports = function(config) {
    var items = {};
    var dynamoRequest = require('./dynamoRequest')(config);

    /**
     * Get an item from a table
     * @param {String} key key of the item to get
     * @param {Object} opts
     * @param {Function} cb callback
     */
    items.getItem = function(key, opts, cb) {
        if (!cb) {
            cb = opts;
            opts = {};
        }
        key = types.toDynamoTypes(key);
        var params = {
            Key: key,
            TableName: opts.table || config.table,
            ConsistentRead: opts.consistentRead || false
        };

        if (opts.capacity) params.ReturnConsumedCapacity = opts.capacity;

        return dynamoRequest('getItem', params, opts, function(err, items, meta) {
            if (err) return cb(err);
            cb(null, _(items).first(), _(meta).first());
        });
    };

    /**
     * Put an item into a table
     * @param {Object} doc document
     * @param {Object} opts
     * @param {Function} cb callback
     */
    items.putItem = function(doc, opts, cb) {
        if (!cb) {
            cb = opts;
            opts = {};
        }
        var item = types.toDynamoTypes(doc);
        var params = {
            TableName: opts.table || config.table,
            Item: item
        };

        if (opts.expected) params.Expected = types.conditions(opts.expected);
        if (opts.conditionalOperator) params.ConditionalOperator = opts.conditionalOperator;
        if (opts.capacity) params.ReturnConsumedCapacity = opts.capacity;

        return dynamoRequest('putItem', params, opts, function(err, resp, meta) {
            if (err) return cb(err);
            cb(null, doc, _(meta).first());
        });
    };

    /**
     * Update an item in a table
     * @param {String} key
     * @param {Object} doc document
     * @param {Object} opts
     * @param {Function} cb callback
     */
    items.updateItem = function(key, doc, opts, cb) {
        if (!cb) {
            cb = opts;
            opts = {};
        }
        var attrs = types.toAttributeUpdates(doc);
        var params = {
            TableName: opts.table || config.table,
            Key: types.toDynamoTypes(key),
            AttributeUpdates: attrs,
            ReturnValues: 'ALL_NEW'
        };

        if (opts.expected) params.Expected = types.conditions(opts.expected);
        if (opts.conditionalOperator) params.ConditionalOperator = opts.conditionalOperator;
        if (opts.capacity) params.ReturnConsumedCapacity = opts.capacity;

        return dynamoRequest('updateItem', params, opts, function(err, items, meta) {
            if (err) return cb(err);
            cb(null, _(items).first(), _(meta).first());
        });
    };

    /**
     * Delete an item from a table
     * @param {String} key
     * @param {Object} opts
     * @param {Function} cb callback
     */
    items.deleteItem = function(key, opts, cb) {
        if (!cb) {
            cb = opts;
            opts = {};
        }

        var params = {
            TableName: opts.table || config.table,
            Key: types.toDynamoTypes(key),
            ReturnValues: 'NONE'
        };
        if (opts.expected) params.Expected = types.conditions(opts.expected);
        if (opts.conditionalOperator) params.ConditionalOperator = opts.conditionalOperator;
        if (opts.capacity) params.ReturnConsumedCapacity = opts.capacity;

        return dynamoRequest('deleteItem', params, opts, function(err, items, meta) {
            if (err) return cb(err);
            cb(null, _(items).first(), _(meta).first());
        });
    };

    /**
     * Estimate the total size of an item, including any local secondary index items
     * @param {object} doc - the item
     * @param {object} tabledef - JSON object defining the table's schema
     * @return {number} the number of bytes required to store the item
     */
    items.estimateSize = function(doc, tabledef) {
        doc = JSON.parse(require('..').serialize(doc));
        var range = tabledef.KeySchema.filter(function(key) { return key.KeyType === 'RANGE'; })[0];

        // http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html
        // For each local secondary index on a table, there is a 400 KB limit on the total size of the following:
        // - The size of an item's data in the table.
        // - The size of the local secondary index entry corresponding to that item, including its key values and projected attributes.
        var lsis = (tabledef.LocalSecondaryIndexes || []).map(function(lsi) {
            var result = {
                hash: lsi.KeySchema.filter(function(key) { return key.KeyType === 'HASH'; })[0].AttributeName,
                index: lsi.KeySchema.filter(function(key) { return key.KeyType === 'RANGE'; })[0].AttributeName
            };

            if (range)
                result.range = range.AttributeName;
            if (lsi.Projection.ProjectionType === 'ALL')
                result.projection = 'all';
            if (lsi.Projection.ProjectionType === 'INCLUDE')
                result.projection = lsi.Projection.NonKeyAttributes;

            return result;
        });

        var docSize = itemSize(0, doc);
        lsis.forEach(function(lsi) {
            var size = lsiSize(doc, lsi);
            docSize += size;
        });

        return docSize;
    };

    return items;
};

function itemSize(total, doc) {
    return Object.keys(doc).reduce(function(total, key) {
        if (typeof doc[key] === 'object') {
            var type = Object.keys(doc[key])[0];
            var val = doc[key][type];
            total += attrSize(key, type, val);
        } else {
            total += attrSize([], key, doc[key]);
        }

        return total;
    }, total);
}

function lsiSize(doc, lsi) {
    if (!doc[lsi.index]) return 0;

    // http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/LSI.html#LSI.StorageConsiderations
    // 100 bytes of overhead per index item
    var size = 100;

    // The size in bytes of the table primary key (hash and range key attributes)
    var keys = {};
    if (lsi.range) keys[lsi.range] = doc[lsi.range];
    keys[lsi.hash] = doc[lsi.hash];

    // The size in bytes of the index key attribute
    keys[lsi.index] = doc[lsi.index];

    size = itemSize(size, keys);

    // The size in bytes of the projected attributes (if any)
    if (!lsi.projection) return size;

    if (lsi.projection === 'all') lsi.projection = Object.keys(doc);
    var projection = lsi.projection.reduce(function(projection, key) {
        if (key !== lsi.hash && key !== lsi.range && key !== lsi.index)
            projection[key] = doc[key];
        return projection;
    }, {});

    size += itemSize(0, projection);

    return size;
}

function attrSize(name, type, val) {
    // http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/WorkingWithTables.html#CapacityUnitCalculations
    // The size of an item is the sum of the lengths of its attribute names and values.
    var sum = name.length;
    if (type === 'N' || type === 'S') sum += val.length;
    if (type === 'NS' || type === 'SS') sum += val.reduce(function(sum, num) {
        sum += num.length;
        return sum;
    }, 0);
    if (type === 'B') sum += (new Buffer(val, 'base64')).length;
    if (type === 'BS') sum += val.reduce(function(sum, buf) {
        sum += (new Buffer(buf, 'base64')).length;
        return sum;
    }, 0);

    // The size of a Null or Boolean attribute value is (length of the attribute name + one byte).
    if (type === 'BOOL' || type === 'NULL') sum += 1;

    // An attribute of type List or Map requires 3 bytes of overhead, regardless of its contents.
    if (type === 'L' || type === 'M') sum += 3;
    // If the attribute is non-empty, the size is (length of the attribute name + sum (length of attribute values) + 3 bytes).
    if (type === 'L') sum += val.reduce(itemSize, 0);
    if (type === 'M') sum += itemSize(0, val);

    return sum;
}
