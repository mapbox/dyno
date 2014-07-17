var config = require('./config')();
var types = require('./types');

module.exports.query = function(conditions, opts, cb) {
    if(!cb) { cb = opts; opts = {}; }
    var keyConditions = {};

    for(c in conditions) {
        var con = conditions[c];
        var compare = Object.keys(con)[0];
        keyConditions[c] = {};
        keyConditions[c].ComparisonOperator = compare;
        keyConditions[c].AttributeValueList = [];
        if(!Array.isArray(con[compare])) con[compare] = [con[compare]];
        con[compare].forEach(function(avl){
            keyConditions[c].AttributeValueList.push(types.toDynamoTypes({val:avl}).val);
        });
    }

    var params = {
        TableName: opts.table || config.table,
        KeyConditions: keyConditions
    };

    if(opts.attributes) {
        params.AttributesToGet = opts.attributes;
        params.Select = 'SPECIFIC_ATTRIBUTES';
    } else  {
        params.Select = opts.select || 'ALL_ATTRIBUTES';
    }

    config.dynamo.query(params, function(err, resp) {
        if(err) return cb(err, resp);

        if(resp.Items)
            resp.Items = types.typesFromDynamo(resp.Items);
        cb(null, {count:resp.Count, items: resp.Items});

    });
};
