var _ = require('underscore');
var { marshall, unmarshall } = require('@aws-sdk/util-dynamodb');

module.exports.serialize = function(item) {
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

  return JSON.stringify(marshall(item, {
    convertEmptyValues: false,
    removeUndefinedValues: false,
    convertClassInstanceToMap: false
  }), replacer);
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
  return unmarshall(obj, {
    wrapNumbers: false
  });
};
