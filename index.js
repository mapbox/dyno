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

var types = require('./lib/types');
Dyno.createSet = types.createSet.bind(types);

Dyno.serialize = function(item) {
    return JSON.stringify(types.toDynamoTypes(item));
};

Dyno.deserialize = function(str) {
    function reviver(key, value) {
        if (value.B && Array.isArray(value.B) && _(value.B).every(function(num) {
            return typeof num === 'number';
        })) {
            return { B: new Buffer(value.B) };
        }

        if (value.BS && Array.isArray(value.BS) && _(value.BS).every(function(buf) {
            return Array.isArray(buf) && _(buf).every(function(num) {
                return typeof num === 'number';
            });
        })) {
            return {
                BS: value.BS.map(function(buf) {
                    return new Buffer(buf);
                })
            };
        }

        return value;
    }

    str = JSON.parse(str, reviver);
    str = types.typesFromDynamo(str);
    return str[0];
};

function badconfig(message) {
    var err = new Error(message);
    err.code = 'EBADCONFIG';
    return err;
}
