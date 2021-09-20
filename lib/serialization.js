var _ = require('underscore');
var converter = require('aws-sdk/lib/dynamodb/converter');

module.exports.serialize = function(item) {
  function replacer(key, value) {
    if (Buffer.isBuffer.from(this[key])) return this[key].toString('base64');

    if (this[key].BS &&
      Array.isArray(this[key].BS) &&
      _(this[key].BS).every(function(buf) {
        return Buffer.isBuffer.from(buf);
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

  return JSON.stringify(Object.keys(item).reduce(function(obj, key) {
    obj[key] = converter.input(item[key]);
    return obj;
  }, {}), replacer);
};
module.exports.deserialize = function(str) {
  function reviver(key, value) {
    if (typeof value === 'object' && value.B && typeof value.B === 'string') {
      return { B: new Buffer.from(value.B, 'base64') };
    }

    if (typeof value === 'object' &&
      value.BS &&
      Array.isArray(value.BS))
    {
      return {
        BS: value.BS.map(function(s) {
          return new Buffer.from(s, 'base64');
        })
      };
    }

    return value;
  }

  var obj = JSON.parse(str, reviver);
  return Object.keys(obj).reduce(function(item, key) {
    var value = converter.output(obj[key]);
    item[key] = value;
    return item;
  }, {});
};
