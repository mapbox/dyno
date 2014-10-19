var AWS = require('aws-sdk');
var _ = require('underscore');

module.exports = function(c){
    var config = {};

    _(config).extend(c);

    AWS.config.update({
        accessKeyId: c.accessKeyId,
        secretAccessKey: c.secretAccessKey,
        sessionToken: c.sessionToken,
        region: c.region
    });

    var opts = {};
    if (c.endpoint && c.endpoint !== '') opts.endpoint = new AWS.Endpoint(c.endpoint);

    config.dynamo = new AWS.DynamoDB(opts);
    
    return config;
};
