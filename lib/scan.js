var config = require('./config')();
var types = require('./types');
var dynamoRequest = require('./dynamoRequest');

module.exports.scan = function(opts, cb) {
    if(!cb) { cb = opts; opts = {}; }

    var params = {
        TableName: opts.table || config.table
    };

    if(opts.attributes) {
        params.AttributesToGet = opts.attributes;
        params.Select = 'SPECIFIC_ATTRIBUTES';
    } else  {
        params.Select = opts.select || 'ALL_ATTRIBUTES';
    }

    return dynamoRequest({query:params, func: function(query, callback) {
        config.dynamo.scan(query, function(err, resp){
            if(resp.Items)
                resp.Items = types.typesFromDynamo(resp.Items);
            callback(null, {count:resp.Count, items: resp.Items});
        });
    }}, cb);
};
