var _ = require('underscore');

module.exports = Dyno;

function Dyno(c) {
    var dyno = {};
    var config = require('./lib/config')(c);
    _(dyno).extend(config.dynamo);
    _(dyno).extend(require('./lib/item')(config));
    _(dyno).extend(require('./lib/query')(config));
    _(dyno).extend(require('./lib/scan')(config));
    _(dyno).extend(require('./lib/table')(config));
    _(dyno).extend(require('./lib/batch')(config));
    _(dyno).extend(require('./lib/describe')(config));
    _(dyno).extend(require('./lib/types'));
    return dyno;
}

Dyno.multi = function(readConfig, writeConfig) {
    if (!readConfig.region) throw badconfig('You must specify a read region');
    if (!readConfig.table) throw badconfig('You must specify a read table');
    if (!writeConfig.region) throw badconfig('You must specify a write region');
    if (!writeConfig.table) throw badconfig('You must specify a write table');

    var read = Dyno(readConfig);
    var write = Dyno(writeConfig);

    return require('./lib/multi')(read, write);
};

function badconfig(message) {
    var err = new Error(message);
    err.code = 'EBADCONFIG';
    return err;
}
