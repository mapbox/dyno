var _ = require('underscore');

module.exports = function(c) {
    var dyno = {};
    dyno.config = require('./lib/config')(c);
    dyno = _(dyno).extend(dyno.config.dynamo);
    dyno = _(dyno).extend(require('./lib/item'));
    dyno = _(dyno).extend(require('./lib/table'));
    return dyno;
}
