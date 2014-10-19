var _ = require('underscore');

module.exports = function(c) {
    var dyno = {};
    var config = require('./lib/config')(c);
    _(dyno).extend(config.dynamo);
    _(dyno).extend(require('./lib/item')(config));
    _(dyno).extend(require('./lib/query')(config));
    _(dyno).extend(require('./lib/scan')(config));
    _(dyno).extend(require('./lib/table')(config));
    _(dyno).extend(require('./lib/batch')(config));
    return dyno;
};
