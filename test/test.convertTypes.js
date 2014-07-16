var test = require('tap').test;
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

test('convert sets multiple items', function(t) {
    var item = {set: [6,5,4,3,2,1]};

    item = types.toDynamoTypes(item);
    t.deepEqual(item, {set: {NS: ['6', '5', '4', '3', '2', '1']}});
    t.end();
});

test('convert multiple types', function(t) {
    var item = {string: 'a', number: 6, set:[1,2], set2: ['a', 'b']};

    item = types.toDynamoTypes(item);
    t.deepEqual(item, {string: {S:'a'}, number: {N:6}, set:{NS:[1,2]}, set2: {SS:['a', 'b']}});
    t.end();
});

test('convert update actions', function(t) {
    var item = {put:{string: 'a'}, add:{count: 1}};

    item = types.toAttributeUpdates(item);
    t.deepEqual(item, {string: {Action: 'PUT', Value:{S:'a'}}, count: {Action: 'ADD', Value:{N:'1'}}});
    t.end();
});

test('convert update actions - delete', function(t) {
    var item = {delete:{string: 'a'}};

    item = types.toAttributeUpdates(item);
    t.deepEqual(item, {string: {Action: 'DELETE'}});
    t.end();
});

test('convert update actions - NS', function(t) {
    var item = {add:{set: [1,2]}};

    item = types.toAttributeUpdates(item);
    t.deepEqual(item, {set: {Action: 'ADD', Value:{NS: ['1', '2']}}});
    t.end();
});
