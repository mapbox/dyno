var test = require('tape');
var types = require('../lib/types');

test('convert strings', function(t) {
    var item = {id: 'yo'};

    item = types.toDynamoTypes(item);
    t.deepEqual(item, {id: {S:'yo'}});
    t.end();
});

test('convert booleans', function(t) {
    var item = {bool1: true, bool2: false };

    item = types.toDynamoTypes(item);
    t.deepEqual(item, {bool1: {BOOL: true}, bool2: {BOOL: false}});
    t.end();
});

test('convert null', function(t) {
    var item = {n: null};

    item = types.toDynamoTypes(item);
    t.deepEqual(item, {n: {NULL: true}});
    t.end();
});

test('convert undefined top-level attribute', function(t) {
    var item = {ok: 1, notOk: undefined};
    item = types.toDynamoTypes(item);
    t.deepEqual(item, {ok: {N: '1'}, notOk: undefined});
    t.end();
});

test('convert undefined in a map', function(t) {
    var item = {ok: 1, notOk: {hidden: 'in here', isSome: undefined}};
    item = types.toDynamoTypes(item);
    t.deepEqual(item, {ok: {N: '1'}, notOk: {M: {hidden: {S: 'in here'}, isSome: undefined}}});
    t.end();
});

test('convert undefined in a map in a list', function(t) {
    var item = {ok:1, notOk: [{thereIs: 'an', attrThatIs: undefined}]};
    item = types.toDynamoTypes(item);
    t.deepEqual(item, {ok: {N: '1'}, notOk: {L: [{M: {thereIs: {S: 'an'}, attrThatIs: undefined}}]}});
    t.end();
});

test('convert numbers', function(t) {
    var item = {id: 6};

    item = types.toDynamoTypes(item);
    t.deepEqual(item, {id: {N: '6'}});
    t.end();
});

test('recursive strings', function(t) {
    var item = 'yo';

    item = types.toDynamoTypes(item);
    t.deepEqual(item, 'yo');
    t.end();
});

test('convert binary', function(t) {
    var buffy = new Buffer('hi');
    var item = { id: buffy };

    item = types.toDynamoTypes(item);
    t.deepEqual(item, {id: { B: buffy }});
    t.end();
});

test('convert sets - strings', function(t) {
    var item = {set: types.createSet(['a'], 'S')};

    item = types.toDynamoTypes(item);
    t.deepEqual(item, {set: {SS: ['a']}});
    t.end();
});

test('convert sets - numbers', function(t) {
    var item = {set: types.createSet([6], 'N')};

    item = types.toDynamoTypes(item);
    t.deepEqual(item, {set: {NS: ['6']}});
    t.end();
});

test('convert sets - binary', function(t) {
    var buffy = new Buffer('hi');
    var item = { set: types.createSet([buffy], 'B') };

    item = types.toDynamoTypes(item);
    t.deepEqual(item, {set: { BS: [buffy] }});
    t.end();
});

test('convert sets multiple items', function(t) {
    var item = {set: types.createSet([6, 5, 4, 3, 2, 1], 'N')};

    item = types.toDynamoTypes(item);
    t.deepEqual(item, {set: {NS: ['1', '2', '3', '4', '5', '6']}});
    t.end();
});

test('convert lists - numbers', function(t) {
    var item = {list: [6, 5, 4, 3, 2, 1]};

    item = types.toDynamoTypes(item);
    t.deepEqual(item, { list: { L: [{ N: '6' }, { N: '5' }, { N: '4' }, { N: '3' }, { N: '2' }, { N: '1' }] } });
    t.end();
});

test('convert lists - strings', function(t) {
    var item = {list: ['foo', 'bar']};

    item = types.toDynamoTypes(item);
    t.deepEqual(item, { list: { L: [{ S: 'foo' }, { S: 'bar' }] } });
    t.end();
});

test('convert lists - multiple types', function(t) {
    var item = {list: [1, 'foo', null, false]};

    item = types.toDynamoTypes(item);
    t.deepEqual(item, { list: { L: [{ N: '1' }, { S: 'foo' }, { NULL: true }, { BOOL: false }] } });
    t.end();
});

test('convert maps', function(t) {
    var item = {map: {
        date: 12345,
        foo: 'bar',
        valid: true
    }};

    item = types.toDynamoTypes(item);
    t.deepEqual(item, { map: { M: { date: { N: '12345' }, foo: { S: 'bar' }, valid: { BOOL: true } } } });
    t.end();
});

test('convert multiple types', function(t) {
    var item = {
        string: 'a',
        number: 6,
        set: types.createSet([1, 2], 'N'),
        set2: types.createSet(['a', 'b'], 'S'),
        list: ['a', 2, null, true, false, {foo: 'bar'}],
        bool: true,
        bool2: false,
        map: {
            string: 'a',
            number: 6,
            set: types.createSet([1, 2], 'N'),
            set2: types.createSet(['a', 'b'], 'S'),
            list: ['a', 2, null, true, false, {foo: 'bar'}],
            bool: true,
            bool2: false,
            nested: {
                string: 'a',
                number: 6,
                set: types.createSet([1, 2], 'N'),
                set2: types.createSet(['a', 'b'], 'S'),
                list: ['a', 2, null, true, false, {foo: 'bar'}],
                bool: true,
                bool2: false
            }
        }
    };

    item = types.toDynamoTypes(item);
    t.deepEqual(item, { string: { S: 'a' },
        number: { N: '6' },
        set: { NS: ['1', '2'] },
        set2: { SS: ['a', 'b'] },
        list:
         { L:
            [{ S: 'a' },
              { N: '2' },
              { NULL: true },
              { BOOL: true },
              { BOOL: false },
              { M: { foo: { S: 'bar' } } }] },
        bool: { BOOL: true },
        bool2: { BOOL: false },
        map:
         { M:
            { string: { S: 'a' },
              number: { N: '6' },
              set: { NS: ['1', '2'] },
              set2: { SS: ['a', 'b'] },
              list:
               { L:
                  [{ S: 'a' },
                    { N: '2' },
                    { NULL: true },
                    { BOOL: true },
                    { BOOL: false },
                    { M: { foo: { S: 'bar' } } }] },
              bool: { BOOL: true },
              bool2: { BOOL: false },
              nested:
               { M:
                  { string: { S: 'a' },
                    number: { N: '6' },
                    set: { NS: ['1', '2'] },
                    set2: { SS: ['a', 'b'] },
                    list:
                     { L:
                        [{ S: 'a' },
                          { N: '2' },
                          { NULL: true },
                          { BOOL: true },
                          { BOOL: false },
                          { M: { foo: { S: 'bar' } } }] },
                    bool: { BOOL: true },
                    bool2: { BOOL: false } } } } } });
    t.end();
});

test('convert update actions', function(t) {
    var item = {put:{string: 'a'}, add:{count: 1}};

    item = types.toAttributeUpdates(item);
    t.deepEqual(item, {string: {Action: 'PUT', Value:{S:'a'}}, count: {Action: 'ADD', Value:{N:'1'}}});
    t.end();
});

test('convert update actions - delete', function(t) {
    var item = {delete:['string']};

    item = types.toAttributeUpdates(item);
    t.deepEqual(item, {string: {Action: 'DELETE'}});
    t.end();
});

test('convert update actions - NS', function(t) {
    var item = {add:{set: types.createSet([1, 2], 'N')}};

    item = types.toAttributeUpdates(item);
    t.deepEqual(item, {set: {Action: 'ADD', Value:{NS: ['1', '2']}}});
    t.end();
});

test('convert update actions - delete from SS', function(t) {
    var item = {delete: {newset: types.createSet(['a'], 'S')}};

    item = types.toAttributeUpdates(item);
    t.deepEqual(item, { newset: { Action: 'DELETE', Value: { SS: ['a'] } } });
    t.end();
});

test('convert update actions - add to SS', function(t) {
    var item = {add: {newset: types.createSet(['a'], 'S')}};

    item = types.toAttributeUpdates(item);
    t.deepEqual(item, { newset: { Action: 'ADD', Value: { SS: ['a'] } } });
    t.end();
});

test('convert update actions - delete with array', function(t) {
    var item = {
        delete: ['foo', 'bar']
    };

    item = types.toAttributeUpdates(item);
    t.deepEqual(item, { bar: { Action: 'DELETE' }, foo: { Action: 'DELETE' } });
    t.end();
});

test('convert update actions - delete with null', function(t) {
    var item = {
        delete: {foo: null, bar: null}
    };

    item = types.toAttributeUpdates(item);
    t.deepEqual(item, { bar: { Action: 'DELETE' }, foo: { Action: 'DELETE' } });
    t.end();
});

test('convert update actions - add and delete', function(t) {
    var item = {
        add: {newset: 'a', counter: 5},
        delete: {newset: types.createSet(['a'], 'S')}
    };

    item = types.toAttributeUpdates(item);
    t.deepEqual(item, { counter: { Action: 'ADD', Value: { N: '5' } }, newset: { Action: 'DELETE', Value: { SS: ['a'] } } });
    t.end();
});

test('typesFromDynamo - undefined', function(t) {
    var items;

    item = types.typesFromDynamo(items);
    t.deepEqual(item, undefined);
    t.end();
});

test('typesFromDynamo - single item', function(t) {
    var items = {id: { S: 'id:2' }};

    item = types.typesFromDynamo(items);
    t.deepEqual(item, [{id: 'id:2'}]);
    t.end();
});

test('typesFromDynamo - item', function(t) {
    var items = [{
        id: { S: 'id:2' },
        range: { N: '2' },
        buffer: { B: new Buffer('hi') },
        list: {L: [{N: '6'}, {N: '5'}, {N: '4'}, {N: '3'}, {N: '2'}, {N: '1'}]}
    }];

    item = types.typesFromDynamo(items);
    t.deepEqual(item, [{buffer: new Buffer('hi'), id: 'id:2', list: [6, 5, 4, 3, 2, 1], range: 2}]);
    t.end();
});

test('typesFromDynamo - set', function(t) {
    var items = [{
        set: {NS: ['6', '5', '4', '3', '2', '1']}
    }];

    item = types.typesFromDynamo(items);
    t.deepEqual(item[0].set.contents, types.createSet([6, 5, 4, 3, 2, 1], 'N').contents);
    t.end();
});
