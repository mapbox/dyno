var test = require('tape');
var testTables = require('./test-tables');
var dynamodb = require('dynamodb-test')(test, 'dyno', testTables.idhash);
var Dyno = require('..');
var Update = require('../lib/update');
var _ = require('underscore');

var fixtures = _.range(10).map(function (i) {
  return {
    id: i.toString(),
    idPower2: Math.pow(i, 2),
    text: 'string'
  };
});

test('[update] properties', function (assert) {
  var update = Update(dynamodb.dynamodb);
  assert.equal(typeof update.dynamicUpdate, 'function', 'exposes dynamicUpdate function');
  assert.end();
});

dynamodb.start();

dynamodb.test('[update] dynamicUpdate - update all fields', fixtures, function (assert) {
  var dyno = Dyno({
    table: dynamodb.tableName,
    region: 'local',
    endpoint: 'http://localhost:4567'
  });

  var newObject = {
    id: '2',
    idPower2: -99,
    text: 'update string'
  };
  var updateParams = {
    TableName: dynamodb.tableName,
    Key: { id: '2' },
    ReturnValues: 'ALL_NEW'
  };

  dyno.dynamicUpdate(newObject, updateParams, function (err, response) {
    assert.ifError(err, 'dynamicUpdate errored');
    assert.deepEqual(response, { Attributes: { id: '2', idPower2: -99, text: 'update string' } }, 'expected new values');
    assert.end();
  });
});

dynamodb.test('[update] dynamicUpdate - only add new field', fixtures, function (assert) {
  var dyno = Dyno({
    table: dynamodb.tableName,
    region: 'local',
    endpoint: 'http://localhost:4567'
  });

  var newObject = {
    id: '2',
    newDateField: '2017-10-31'
  };
  var updateParams = {
    TableName: dynamodb.tableName,
    Key: { id: '0' },
    ReturnValues: 'ALL_NEW'
  };

  var expectedRow = { id: '0', idPower2: 0, newDateField: '2017-10-31', text: 'string' };
  dyno.dynamicUpdate(newObject, updateParams, function (err, response) {
    assert.ifError(err, 'dynamicUpdate errored');
    assert.deepEqual(response, { Attributes: expectedRow }, 'expected new values');
    dyno.query({ KeyConditionExpression: 'id = :id', ExpressionAttributeValues: { ':id': '0' } }, function (err, queryResponse) {
      assert.ifError(err, 'query error');
      assert.deepEqual(queryResponse.Items, [expectedRow], 'expected query response');
      assert.end();
    });
  });
});

dynamodb.delete();
dynamodb.close();