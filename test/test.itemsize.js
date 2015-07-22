var test = require('tape');
var fixtures = require('./fixtures');
var _ = require('underscore');
var dyno = require('..')({ table: 'fake' });

test('[item size] simple, no LSI', function(assert) {
    var table = _.clone(fixtures.test);

    // attribute names + hash/range values should add 16 to the byte count
    var item = fixtures.randomItems(1, 100)[0];
    var found = dyno.estimateSize(item, table);
    assert.equal(found, 116, 'correct size estimation');

    item = fixtures.randomItems(1, 1000)[0];
    found = dyno.estimateSize(item, table);
    assert.equal(found, 1016, 'correct size estimation');

    assert.end();
});

test('[item size] simple, LSI KEYS_ONLY', function(assert) {
    var table = _.clone(fixtures.test);

    table.AttributeDefinitions.push({
        AttributeName: 'index',
        AttributeType: 'S'
    });

    table.LocalSecondaryIndexes = [
        {
            IndexName: 'index',
            KeySchema: [
                {
                    AttributeName: 'id',
                    KeyType: 'HASH'
                },
                {
                    AttributeName: 'index',
                    KeyType: 'RANGE'
                }
            ],
            Projection: {
                ProjectionType: 'KEYS_ONLY'
            }
        }
    ];

    var item = fixtures.randomItems(1, 100)[0];
    item.index = 'abc';

    // Should end up with:
    // - 100 + 16 + 8 for the record
    // - 100 lsi overhead
    // - hash/range/index = 6/6/8
    // ==> 244
    found = dyno.estimateSize(item, table);
    assert.equal(found, 244, 'correct size estimation');
    assert.end();
});

test('[item size] simple, LSI ALL', function(assert) {
    var table = _.clone(fixtures.test);

    table.AttributeDefinitions.push({
        AttributeName: 'index',
        AttributeType: 'S'
    });

    table.LocalSecondaryIndexes = [
        {
            IndexName: 'index',
            KeySchema: [
                {
                    AttributeName: 'id',
                    KeyType: 'HASH'
                },
                {
                    AttributeName: 'index',
                    KeyType: 'RANGE'
                }
            ],
            Projection: {
                ProjectionType: 'ALL'
            }
        }
    ];

    var item = fixtures.randomItems(1, 100)[0];
    item.index = 'abc';

    // Should end up with:
    // - 100 + 16 + 8 for the record
    // - 100 lsi overhead
    // - 100 + 16 + 8 again for the projected record
    // ==> 348
    found = dyno.estimateSize(item, table);
    assert.equal(found, 348, 'correct size estimation');
    assert.end();
});

test('[item size] simple, LSI INCLUDE', function(assert) {
    var table = _.clone(fixtures.test);

    table.AttributeDefinitions.push({
        AttributeName: 'index',
        AttributeType: 'S'
    });

    table.LocalSecondaryIndexes = [
        {
            IndexName: 'index',
            KeySchema: [
                {
                    AttributeName: 'id',
                    KeyType: 'HASH'
                },
                {
                    AttributeName: 'index',
                    KeyType: 'RANGE'
                }
            ],
            Projection: {
                ProjectionType: 'INCLUDE',
                NonKeyAttributes: [
                    'extra'
                ]
            }
        }
    ];

    var item = fixtures.randomItems(1, 100)[0];
    item.index = 'abc';
    item.extra = '123';

    // Should end up with:
    // - 100 + 16 + 8 + 8 for the record
    // - 100 lsi overhead
    // - hash/range/index = 6/6/8
    // - 8 for the projected attribute
    // ==> 260
    found = dyno.estimateSize(item, table);
    assert.equal(found, 260, 'correct size estimation');
    assert.end();
});

test('[item size] simple, LSI avoided', function(assert) {
    var table = _.clone(fixtures.test);

    table.AttributeDefinitions.push({
        AttributeName: 'index',
        AttributeType: 'S'
    });

    table.LocalSecondaryIndexes = [
        {
            IndexName: 'index',
            KeySchema: [
                {
                    AttributeName: 'id',
                    KeyType: 'HASH'
                },
                {
                    AttributeName: 'index',
                    KeyType: 'RANGE'
                }
            ],
            Projection: {
                ProjectionType: 'KEYS_ONLY'
            }
        }
    ];

    var item = fixtures.randomItems(1, 100)[0];

    // Should end up with:
    // - 100 + 16 for the record
    // ==> 116
    found = dyno.estimateSize(item, table);
    assert.equal(found, 116, 'correct size estimation');
    assert.end();
});

test('[item size] complex item', function(assert) {
    var table = _.clone(fixtures.test);
    var Dyno = require('..');

    var item = fixtures.randomItems(1, 100)[0];    // 116
    item.e = true;                                   // 2
    item.f = null;                                   // 2
    item.a = 'a';                                    // 2
    item.b = 1;                                      // 2
    item.g = new Buffer('a');                        // 2
    item.c = Dyno.createSet(['a', 'b'], 'S');        // 3
    item.d = Dyno.createSet([1, 2], 'N');            // 3
    item.h = Dyno.createSet([new Buffer('a')], 'B'); // 2
    item.i = ['a', 2, new Buffer('a')];              // 7
    item.j = { a: 'a' };                             // 6
                                                   // ---
                                                   // 147
    found = dyno.estimateSize(item, table);
    assert.equal(found, 147, 'correct size estimation');
    assert.end();
});
