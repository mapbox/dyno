var config = require('./config')();
var types = require('./types');
var dynamoRequest = require('./dynamoRequest');
var _ = require('underscore');

module.exports.query = function(conditions, opts, cb) {
    if(!cb && _.isFunction(opts)) { cb = opts; opts = {}; }
    if(!opts) opts ={};

    var keyConditions = prepareConditions(conditions);
    var params = {
        TableName: opts.table || config.table,
        KeyConditions: keyConditions
    };
    if(opts.filter) {
        params.QueryFilter = prepareConditions(opts.filter);
    }
    if(opts.index) {
        params.IndexName = opts.index;
    }
    if(opts.limit) {
        params.Limit = opts.limit;
    }
    if(opts.start) {
        params.ExclusiveStartKey = opts.start;
    }
    if(opts.attributes) {
        params.AttributesToGet = opts.attributes;
        params.Select = 'SPECIFIC_ATTRIBUTES';
    } else  {
        params.Select = opts.select || 'ALL_PROJECTED_ATTRIBUTES';
    }
    return dynamoRequest({query: params, pages: opts.pages, func: function(query, callback) {
        config.dynamo.query(query, function(err, resp){
            if (err) return callback(err);
            if(resp.Items)
                resp.Items = types.typesFromDynamo(resp.Items);
            var result = { count:resp.Count, items: resp.Items };
            if(resp.LastEvaluatedKey)
                result.last = resp.LastEvaluatedKey;
            callback(null, result);
        });
    }}, cb);
};

function prepareConditions(conditions) {
    var preparedConditions = {};

    for(c in conditions) {
        var con = conditions[c];
        var compare = Object.keys(con)[0];
        preparedConditions[c] = {};
        preparedConditions[c].ComparisonOperator = compare;
        preparedConditions[c].AttributeValueList = [];
        if(!Array.isArray(con[compare])) con[compare] = [con[compare]];
        con[compare].forEach(function(avl){
            preparedConditions[c].AttributeValueList.push(types.toDynamoTypes({val:avl}).val);
        });
    }
    return preparedConditions;
}
