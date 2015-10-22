var AWS = require('aws-sdk');
var _ = require('underscore');

module.exports = function(c) {
    var config = {};

    _(config).extend(c);

    if (!config.dynamo) {
      var opts = { region: c.region };
      if (c.endpoint && c.endpoint !== '') opts.endpoint = new AWS.Endpoint(c.endpoint);
      if (c.httpOptions) opts.httpOptions = c.httpOptions;

      config.dynamo = new AWS.DynamoDB(opts);
      config.dynamo.config.update({
          accessKeyId: c.accessKeyId,
          secretAccessKey: c.secretAccessKey,
          sessionToken: c.sessionToken
      });
    }

    if (c.kinesisConfig) {
        if (!c.kinesisConfig.stream) throw badconfig('You must specify a kinesis stream');
        if (!c.kinesisConfig.region) throw badconfig('You must specify a region for the kinesis stream');
        if (!Array.isArray(c.kinesisConfig.key)) throw badconfig('You must specify key attributes for the kinesis stream');
        if (!c.table) throw badconfig('You must specify the name of the table that feeds the kinesis stream');

        config.kinesis = new AWS.Kinesis(c.kinesisConfig);
    }

    return config;
};

function badconfig(message) {
    var err = new Error(message);
    err.code = 'EBADCONFIG';
    return err;
}
