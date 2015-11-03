var test = require('tape');
var testTables = require('./test-tables');
var mock = require('dynamodb-test')(test, 'dyno', testTables.idhash);
var Dyno = require('..');
var Table = require('../lib/table');
var _ = require('underscore');

mock.start();

test('[table] properties', function(assert) {
  var table = Table(mock.dynamo);
  assert.equal(typeof table.create, 'function', 'exposes create function');
  assert.equal(typeof table.delete, 'function', 'exposes delete function');
  assert.equal(typeof table.multiCreate, 'function', 'exposes multiCreate function');
  assert.equal(typeof table.multiDelete, 'function', 'exposes multiDelete function');
  assert.end();
});

test('[table] create table no-op when already exists', function(assert) {
  var dyno = Dyno({
    table: mock.tableName,
    region: 'local',
    endpoint: 'http://localhost:4567'
  });

  dyno.createTable(_({ TableName: mock.tableName }).extend(testTables.idhash), function(err, data) {
    assert.ifError(err, 'success');
    assert.ok(data.TableDefinition, 'response contains TableDefinition');

    mock.dynamo.listTables(function(err, data) {
      assert.equal(data.TableNames.length, 1, 'does not create a new table');
      assert.end();
    });
  });
});

mock.delete();

test('[table] create table that does not exist', function(assert) {
  var dyno = Dyno({
    table: 'new-table',
    region: 'local',
    endpoint: 'http://localhost:4567'
  });

  dyno.createTable(_({ TableName: 'new-table' }).extend(testTables.idhash), function(err, data) {
    assert.ifError(err, 'success');
    assert.ok(data.TableDefinition, 'response contains TableDefinition');

    mock.dynamo.listTables(function(err, data) {
      assert.deepEqual(data.TableNames, ['new-table'], 'create a new table');
      assert.end();
    });
  });
});

test('[table] delete table that does exist', function(assert) {
  var dyno = Dyno({
    table: 'new-table',
    region: 'local',
    endpoint: 'http://localhost:4567'
  });

  dyno.deleteTable({ TableName: 'new-table' }, function(err, data) {
    assert.ifError(err, 'success');
    assert.notOk(data, 'no data returned'); // departs from native function

    mock.dynamo.listTables(function(err, data) {
      assert.equal(data.TableNames.length, 0, 'deletes a table');
      assert.end();
    });
  });
});

test('[table] delete table no-op on table that does not exist', function(assert) {
  var dyno = Dyno({
    table: 'dne',
    region: 'local',
    endpoint: 'http://localhost:4567'
  });

  dyno.deleteTable({ TableName: 'dne' }, function(err, data) {
    assert.ifError(err, 'success');
    assert.notOk(data, 'no data returned'); // departs from native function
    assert.end();
  });
});

test('[table] multi table create', function(assert) {
  var dyno = Dyno.multi(
    { table: 'one', region: 'local', endpoint: 'http://localhost:4567' },
    { table: 'two', region: 'local', endpoint: 'http://localhost:4567' }
  );

  dyno.createTable(testTables.idhash, function(err, data) {
    assert.ifError(err);
    assert.notOk(data, 'no data returned');

    mock.dynamo.listTables(function(err, data) {
      assert.deepEqual(data.TableNames, ['one', 'two'], 'creates two tables');
      assert.end();
    });
  });
});

test('[table] multi delete table', function(assert) {
  var dyno = Dyno.multi(
    { table: 'one', region: 'local', endpoint: 'http://localhost:4567' },
    { table: 'two', region: 'local', endpoint: 'http://localhost:4567' }
  );

  dyno.deleteTable(function(err, data) {
    assert.ifError(err);
    assert.notOk(data, 'no data returned');

    mock.dynamo.listTables(function(err, data) {
      assert.equal(data.TableNames.length, 0, 'deletes two tables');
      assert.end();
    });
  });
});

mock.close();
