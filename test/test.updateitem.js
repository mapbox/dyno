// var dyno = require('../');
// var test = require('tap').test;
//
// test('update Item', function(t) {
//
//     var item = {id: 'yo', range: 5};
//     dyno.updateItem(item, function(err, resp){
//         t.equal(err, null);
//         t.equal(resp, {});
//         t.end();
//     });
//
//
//
// });
//
// test('update Item - defaults to put', function(t) {
//
//     var item = {id: 'yo', range: 5};
//     var d = dyno.updateItem(item, {
//         expects: { "range": {
//                 "ComparisonOperator": "EQ",
//                 "AttributeValueList" : [5]
//             }}
//     }, function(err, resp){
//         t.equal(err, {});
//         t.equal(resp, {});
//         t.end();
//     });
//     t.equal(d.query, {})
// });
//
// test('update Item - defaults to put', function(t) {
//
//     var actions = {put: {id: 'yo', range: 5}, delete: ['otherrange'], add: {counter: 1}};
//     var d = dyno.updateItem(item, {
//         expects: { "range": {
//                 "ComparisonOperator": "EQ",
//                 "AttributeValueList" : [5]
//             }}
//     }, function(err, resp){
//         t.equal(err, {});
//         t.equal(resp, {});
//         t.end();
//     });
//     t.equal(d.query, {})
// });
//
// test('update Item - with condition', function(t) {
//
//     var item = {id: 'yo', range: 5};
//     dyno.updateItem(item, {
//         expects: { "range": {
//                 "ComparisonOperator": "EQ",
//                 "AttributeValueList" : [5]
//             }}
//     }, function(err, resp){
//         t.equal(err, {});
//         t.equal(resp, {});
//         t.end();
//     });
//
// });
//
// test('update Item - with condition. conditionalCheckFailedOk', function(t) {
//
//     var itemAdds = {id: 'yo', range: 5};
//     dyno.updateItem(item, {
//         conditionalCheckFailedOk : true,
//         expects: {"range": {
//                 "ComparisonOperator": "EQ",
//                 "AttributeValueList" : [5]
//             }}
//     }, function(err, resp){
//         t.equal(err, {});
//         t.equal(resp, {});
//         t.end();
//     });
// });
//
// test('update Item - with condition. conditionalCheckFailedOk', function(t) {
//
//     var itemPuts = {id: 'yo', range: 5};
//     var itemDeletes = ['a', 'b'];
//
//     dyno.updateItem({put:itemPuts, delete:itemDeletes}, {
//         conditionalCheckFailedOk : true,
//         expects: { "range": {
//                 "ComparisonOperator": "EQ",
//                 "AttributeValueList" : [5]
//             }}
//     }, function(err, resp){
//         t.equal(err, {});
//         t.equal(resp, {});
//         t.end();
//
//     });
// });
