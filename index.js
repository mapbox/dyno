var _ = require('underscore');

module.exports = function(c) {
    var dyno = {};
    dyno.config = require('./lib/config')(c);
    _(dyno).extend(dyno.config.dynamo);
    _(dyno).extend(_(require('./lib/item')).omit('_response'));
    _(dyno).extend(require('./lib/query'));
    _(dyno).extend(require('./lib/scan'));
    _(dyno).extend(require('./lib/table'));
    return dyno;
}
