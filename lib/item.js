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
     * @param {Object} doc - document to put to the DynamoDB table
     * @param {Object} [opts] - options that modify item put and are passed to the DynamoDB request
     * @param {String} [opts.table] - table to use for the update if provided; otherwise uses the value from Dyno client config
     * @param {String} [opts.expected] - attribute/condition pairs consisting of an attribute name, a comparison operator, and one or more values
     * @param {String} [opts.conditionalOperator] - logical operator to apply to the conditions in `expected`
     * @param {Boolean} [opts.capacity] - indicates that the DynamoDB response should include the capacity consumed by the update operation
     * @param {Function} callback
     * @example
     * dyno.putItem({
     *     id: 'yo',
     *     range: 5,
     *     subject: 'test'
     * }, {
     *      expected: {
     *          likes: { 'LE': 1000 }
     *      }
     * }, function(err, resp) {
     *     // called asynchronously
     * });
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
     * @param {String} key - the primary key of the item to be updated
     * @param {Object} updates - the names of attributes to be modified, the action to perform on each, and the new value for each
     * @param {Object} [opts] - options that modify item update and are passed to the DynamoDB request
     * @param {String} [opts.table] - table to use for the update if provided; otherwise uses the value from Dyno client config
     * @param {String} [opts.expected] - attribute/condition pairs consisting of an attribute name, a comparison operator, and one or more values
     * @param {String} [opts.conditionalOperator] - logical operator to apply to the conditions in `expected`
     * @param {Boolean} [opts.capacity] - indicates that the DynamoDB response should include the capacity consumed by the update operation
     * @param {Function} callback
     * @example
     * dyno.updateItem({
     *     id: 'yo',
     *     range: 5
     * }, {
     *     put: {
     *         subject: 'oh hai',
     *         message: 'kthxbai'
     *     },
     *     add: { likes: 1 }
     * }, {
     *      expected: {
     *          likes: { 'LE': 1000 }
     *      }
     * }, function(err, resp) {
     *     // called asynchronously
     * });
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

    return items;
};
