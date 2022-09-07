/* eslint-env es6 */
const test = require('tape');
const reduceCapacity = require('../lib/util').reduceCapacity;

test('[reduceCapacity] parses new data format correctly', function (assert) {
  const src = [{
    TableName: 'db-staging',
    CapacityUnits: 8,
    Table: { CapacityUnits: 4 },
    GlobalSecondaryIndexes: { 'id-index': { CapacityUnits: 4 } }
  }];
  const dst = {};

  reduceCapacity(dst, src);

  assert.equal(dst.CapacityUnits, 8);
  assert.equal(dst.Table.CapacityUnits, 4);
  assert.equal(dst.GlobalSecondaryIndexes['id-index'].CapacityUnits, 4);
  assert.end();
});

test('[reduceCapacity] parses old data format correctly', function (assert) {
  const src = {
    TableName: 'db-staging',
    CapacityUnits: 8,
    Table: { CapacityUnits: 4 },
    GlobalSecondaryIndexes: { CapacityUnits: 4 }
  };
  const dst = {};

  reduceCapacity(dst, src);

  assert.equal(dst.CapacityUnits, 8);
  assert.equal(dst.Table.CapacityUnits, 4);
  assert.equal(dst.GlobalSecondaryIndexes.CapacityUnits, 4);
  assert.end();
});

test('[reduceCapacity] merges old data format correctly', function (assert) {
  const src = [{
    TableName: 'db-staging',
    CapacityUnits: 8,
    Table: { CapacityUnits: 4 },
    GlobalSecondaryIndexes: { 'id-index': { CapacityUnits: 4 } }
  }];
  const dst = {
    CapacityUnits: 2,
    Table: { CapacityUnits: 1 },
    GlobalSecondaryIndexes: { 'di-index': { CapacityUnits: 4 } }
  };

  reduceCapacity(dst, src);

  assert.equal(dst.CapacityUnits, 10);
  assert.equal(dst.Table.CapacityUnits, 5);
  assert.equal(dst.GlobalSecondaryIndexes['id-index'].CapacityUnits, 4);
  assert.equal(dst.GlobalSecondaryIndexes['di-index'].CapacityUnits, 4);
  assert.end();
});
