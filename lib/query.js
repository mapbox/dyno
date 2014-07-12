var config = require('./config')();


// overide this in the future
module.exports.query = config.dynamo.query;
