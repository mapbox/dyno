var config = require('./config')();

// overide this in the future
module.exports.scan = config.dynamo.scan;
