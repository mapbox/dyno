var test = require('tape');
var types = require('../lib/types');

test('convert strings', function(t) {
    var item = {id: 'yo'};

    item = types.toDynamoTypes(item);
    t.deepEqual(item, {id: {S:'yo'}});
    t.end();

});

test('convert numbers', function(t) {
    var item = {id: 6};

    item = types.toDynamoTypes(item);
    t.deepEqual(item, {id: {N: '6'}});
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
    var item = {set: ['a']};

    item = types.toDynamoTypes(item);
    t.deepEqual(item, {set: {SS: ['a']}});
    t.end();
});

test('convert sets - numbers', function(t) {
    var item = {set: [6]};

    item = types.toDynamoTypes(item);
    t.deepEqual(item, {set: {NS: ['6']}});
    t.end();
});

test('convert sets - binary', function(t) {
    var buffy = new Buffer('hi');
    var item = { set: [buffy] };

    item = types.toDynamoTypes(item);
    t.deepEqual(item, {set: { BS: [buffy] }});
    t.end();
});

test('convert sets multiple items', function(t) {
    var item = {set: [6, 5, 4, 3, 2, 1]};

    item = types.toDynamoTypes(item);
    t.deepEqual(item, {set: {NS: ['6', '5', '4', '3', '2', '1']}});
    t.end();
});

test('convert multiple types', function(t) {
    var item = {string: 'a', number: 6, set:[1, 2], set2: ['a', 'b']};

    item = types.toDynamoTypes(item);
    t.deepEqual(item, {string: {S:'a'}, number: {N:'6'}, set:{NS:['1', '2']}, set2: {SS:['a', 'b']}});
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
    var item = {add:{set: [1, 2]}};

    item = types.toAttributeUpdates(item);
    t.deepEqual(item, {set: {Action: 'ADD', Value:{NS: ['1', '2']}}});
    t.end();
});

test('convert update actions - delete from SS', function(t) {
    var item = {delete: {newset: ['a']}};

    item = types.toAttributeUpdates(item);
    t.deepEqual(item, { newset: { Action: 'DELETE', Value: { SS: ['a'] } } });
    t.end();
});

test('convert update actions - add to SS', function(t) {
    var item = {add: {newset: ['a']}};

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
        delete: {newset: ['a']}
    };

    item = types.toAttributeUpdates(item);
    t.deepEqual(item, { counter: { Action: 'ADD', Value: { N: '5' } }, newset: { Action: 'DELETE', Value: { SS: ['a'] } } });
    t.end();
});
