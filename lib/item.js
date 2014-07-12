var util = require('util');
var _ = require('underscore');
var items = module.exports = {};
var types = require('./types');
var config = require('./config')();

// Delete all items in a table. Most of the time you probably just want to
// delete the table.
items.deleteAll = function(opts, cb) {

    var tableName = config.tablePrefix + '-' + opts.tableName;
    console.log('clearing table contents ', tableName);

    var params = {
        TableName:tableName,
        AttributesToGet:['id', 'period']
    };

    config.dynamo.scan(params, function(err, data) {
        if(err) throw err;

        if(data.Items.length === 0)
            return cb();

        var count = 0;
        data.Items.forEach(function(item) {

            var deleteItem = {
                TableName:tableName,
                Key: {
                    'id':item.id,
                    'period':item.period
                }
            };
            config.dynamo.deleteItem(deleteItem, function(err) {
                if(err) throw err;
                count++;
                if(data.Items.length === count) cb();
            });
        });
    });
};


items.getItem = function(key, opts, cb){
    if(!cb){ cb = opts; opts = {}; }
    key = types.toDynamoTypes(key);
    var params = {
        Key: key,
        TableName: opts.table || config.table,
        ConsistentRead: opts.consistentRead || false
    };
    config.dynamo.getItem(params, function(err, resp) {
        resp.Item = types.typesFromDynamo(resp.Item)
        if(resp && resp.Item && resp.Item.length === 1)
            resp.Item = resp.Item[0];
        cb(err, resp);
    });
};

items.putItem = function(doc, opts, cb) {
    if(!cb) { cb = opts; opts ={};}
    var item = types.toDynamoTypes(doc);
    var params = {
        TableName: opts.table || config.table,
        Item: item
    };
    config.dynamo.putItem(params, function(err, resp){
        cb(err, resp)
    });
};

items.updateItem = function(doc, opts, cb) {
    var attrs = types.convertToAttributeUpdates(_.omit(doc, ['id', 'period']));

    var params = {
        TableName: config.tablePrefix + '-' + opts.tableName,
        Key: {
            id:{ S: doc.id },
            period:{ N: doc.period.toString() }
        },
        AttributeUpdates: attrs
    };
    config.dynamo.updateItem(params, cb);
};

var convertToAttributeUpdates = function(attrs, action) {
    var attrUpdates = {};

    for(var key in attrs){
        var val = attrs[key];

        if(key === 'id') {
            attrUpdates[key] = {Action:'PUT',
                                Value:{S:val.toString()}};
        } else if(key === '_tf') {
            attrUpdates[key] = {Action:'ADD',
                                Value:{NS: []}};
            val.forEach(function(v){
                attrUpdates[key].Value.NS.push(v.toString());
            });

        } else {
            attrUpdates[key] ={Action:'ADD',
                               Value:{N:val.toString()}};
        }
        if(action){
            attrUpdates[key].Action = action;
            if(action === 'DELETE') {
                attrUpdates[key] = _(attrUpdates[key]).omit('Value');
            }
        }
    }
    return attrUpdates;
};
