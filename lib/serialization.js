var _ = require('underscore');
var converter = require('aws-sdk/lib/dynamodb/converter');
var DynamoDBSet = require('aws-sdk/lib/dynamodb/set');

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

  return JSON.stringify(Object.keys(item).reduce(function(obj, key) {
    obj[key] = converter.input(item[key]);
    return obj;
  }, {}), replacer);
};
module.exports.deserialize = function(str) {
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

  var obj = JSON.parse(str, reviver);
  return Object.keys(obj).reduce(function(item, key) {
    var value = converter.output(obj[key]);

    // hotfix for https://github.com/aws/aws-sdk-js/issues/801
    if (value instanceof DynamoDBSet && !value.type) {
      var typeOf = require('aws-sdk/lib/dynamodb/types').typeOf;
      value.type = typeOf(value.values[0]);
    }

    item[key] = value;
    return item;
  }, {});
};
