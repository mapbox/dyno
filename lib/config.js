var AWS = require('aws-sdk');
var _ = require('underscore');

var config = {};
module.exports = function(c){
    if(!c) return config;

    _(config).extend(c);

    AWS.config.update({
        accessKeyId: c.awsKey,
        secretAccessKey: c.awsSecret,
        region: c.region || 'us-east-1'
    });

    var opts = {};
    if (c.endpoint) opts.endpoint = new AWS.Endpoint(c.endpoint);

    config.dynamo = new AWS.DynamoDB(opts);

    return config;
}
