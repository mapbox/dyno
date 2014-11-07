var AWS = require('aws-sdk');
var _ = require('underscore');

module.exports = function(c){
    var config = {};

    _(config).extend(c);

    // attempt to prime the creds by getting them now instead of on
    // the first request.
    var creds = new AWS.Credentials();
    creds.get();

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
