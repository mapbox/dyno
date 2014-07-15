var _ = require('underscore');

module.exports = function(c) {
    var dyno = {};
    dyno.config = require('./lib/config')(c);
    _(dyno).extend(dyno.config.dynamo);
    _(dyno).extend(require('./lib/item'));
    _(dyno).extend(require('./lib/table'));
    return dyno;
}
