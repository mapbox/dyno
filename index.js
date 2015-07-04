var _ = require('underscore');

module.exports = Dyno;

/** Dyno constructor
 * @param {Object} config - a configuration object
 * @param {String} [config.endpoint] - endpoint to use for DynamoDB client
 * @param {String} [config.httpOptions] - httpOptions passed to DynamoDB client
 * @param {String} [config.kinesisConfig] - kinesisConfig to use for logging write operations
 * @param {String} [config.kinesisConfig.stream]
 * @param {String} [config.kinesisConfig.region]
 * @param {String} [config.kinesisConfig.key]
 * @param {String} [config.accessKeyId]
 * @param {String} [config.secretAccessKey]
 * @param {String} [config.sessionToken]
**/
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
    function replacer(key, value) {
        if (Buffer.isBuffer(this[key])) return this[key].toString('base64');

        if (this[key].BS &&
            Array.isArray(this[key].BS) &&
            _(this[key].BS).every(function(buf) {
                return Buffer.isBuffer(buf);
            }))
        {
            return {
                BS: this[key].BS.map(function(buf) {
                    return buf.toString('base64');
                })
            };
        }

        return value;
    }

    return JSON.stringify(types.toDynamoTypes(item), replacer);
};

Dyno.deserialize = function(str) {
    function reviver(key, value) {
        if (typeof value === 'object' && value.B && typeof value.B === 'string') {
            return { B: new Buffer(value.B, 'base64') };
        }

        if (typeof value === 'object' &&
            value.BS &&
            Array.isArray(value.BS))
        {
            return {
                BS: value.BS.map(function(s) {
                    return new Buffer(s, 'base64');
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
