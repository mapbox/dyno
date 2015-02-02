var AWS = require('aws-sdk');
var _ = require('underscore');


// Dyno config:
/*

  AWS Credentials. Well attempt to grab from Environment / IAM metadata service if not present.
  - accessKeyId
  - secretAccessKey
  - sessionToken

  - region - will be used for both read and write.
  - readRegion - will be used for reads
  - writeRegion - will be used for writes

  - endpoint - will be used for both reads and writes
  - readEndpoint - will be used for reads
  - writeEndpoint - will be used for writes

  - kinesisRegion - region. required if endpoint isnt set.
  - kinesisEndpoint - endpoint, optional
  - kinesisStream - name of the kinesisstream

  - compareEndpoint
  - compareRegion
  - comparePrecentage
  - compareCallback
  - 

*/


module.exports = function(c){
    var config = {};

    _(config).extend(c);

    AWS.config.update({
        accessKeyId: c.accessKeyId,
        secretAccessKey: c.secretAccessKey,
        sessionToken: c.sessionToken,
        region: c.region,
    });

    var opts = {};
    if (c.endpoint && c.endpoint !== '') opts.endpoint = new AWS.Endpoint(c.endpoint);

    config.dynamo = new AWS.DynamoDB(opts);

    if(c.kinesisEndpoint && c.kinesisStream) {
        config.kinesis = new AWS.Kinesis({
            endpoint: config.kinesisEndpoint,
            region: config.kinesisRegion
        });
    }

    return config;
};
