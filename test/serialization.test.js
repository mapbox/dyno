var Dyno = require('..');
var test = require('tape');

var original = {
  str: 'a string',
  num: 2,
  bin: new Buffer('a binary'),
  bool: true,
  nothin: null,
  strSet: Dyno.createSet(['a', 'b', 'c'], 'S'),
  numSet: Dyno.createSet([1, 2, 3], 'N'),
  binSet: Dyno.createSet([new Buffer('a'), new Buffer('b'), new Buffer('c')], 'B'),
  list: ['1', 2, new Buffer('three'), false],
  B: 'a trap',
  map: {
    nested: {
      str: 'a string',
      num: 2,
      bin: new Buffer('a binary'),
      bool: true,
      nothin: null,
      strSet: Dyno.createSet(['a', 'b', 'c'], 'S'),
      numSet: Dyno.createSet([1, 2, 3], 'N'),
      binSet: Dyno.createSet([new Buffer('a'), new Buffer('b'), new Buffer('c')], 'B'),
      list: ['1', 2, new Buffer('three'), false]
    }
  },
  trickyMap: {
    SS: [1, 2, 3],
    BS: ['1', 2, new Buffer('three'), false],
    B: Dyno.createSet(['a', 'b', 'c'], 'S')
  }
};

var expected = {
  str: { S: 'a string' },
  num: { N: '2' },
  bin: { B: 'YSBiaW5hcnk=' },
  bool: { BOOL: true },
  nothin: { NULL: true },
  strSet: {
    SS: ['a', 'b', 'c']
  },
  numSet: {
    NS: ['1', '2', '3']
  },
  binSet: {
    BS: ['YQ==', 'Yg==', 'Yw==']
  },
  list: {
    L: [
      { S: '1' },
      { N: '2' },
      { B: 'dGhyZWU=' },
      { BOOL: false }
    ]
  },
  B: { S: 'a trap' },
  map: {
    M: {
      nested: {
        M: {
          str: { S: 'a string' },
          num: { N: '2' },
          bin: { B: 'YSBiaW5hcnk=' },
          bool: { BOOL: true },
          nothin: { NULL: true },
          strSet: {
            SS: ['a', 'b', 'c']
          },
          numSet: {
            NS: ['1', '2', '3']
          },
          binSet: {
            BS: ['YQ==', 'Yg==', 'Yw==']
          },
          list: {
            L: [
              { S: '1' },
              { N: '2' },
              { B: 'dGhyZWU=' },
              { BOOL: false }
            ]
          }
        }
      }
    }
  },
  trickyMap: {
    M: {
      SS: {
        L: [
          { N: '1' },
          { N: '2' },
          { N: '3' }
        ]
      },
      BS: {
        L: [
          { S: '1' },
          { N: '2' },
          { B: 'dGhyZWU=' },
          { BOOL: false }
        ]
      },
      B: {
        SS: ['a', 'b', 'c']
      }
    }
  }
};

test('[serialization]', function(assert) {
  var str = Dyno.serialize(original);
  assert.ok(typeof str === 'string', 'serializes to a string');
  assert.equal(str, JSON.stringify(expected), 'expected serialized string');

  var roundtrip = Dyno.deserialize(str);
  assert.ok(typeof roundtrip === 'object', 'deserializes from a string');

  assert.equal(roundtrip.str, original.str, 'round-trips str attribute');
  assert.equal(roundtrip.num, original.num, 'round-trips num attribute');
  assert.deepEqual(roundtrip.bin, original.bin, 'round-trips bin attribute');
  assert.equal(roundtrip.bool, original.bool, 'round-trips bool attribute');
  assert.equal(roundtrip.nothin, original.nothin, 'round-trips null attribute');
  assert.equal(roundtrip.B, original.B, 'round-trips str attribute called B');

  assert.equal(roundtrip.strSet.datatype, original.strSet.datatype, 'round-trips SS datatype');
  assert.deepEqual(roundtrip.strSet.contents, original.strSet.contents, 'round-trips SS contents');
  assert.equal(roundtrip.numSet.datatype, original.numSet.datatype, 'round-trips NS datatype');
  assert.deepEqual(roundtrip.numSet.contents, original.numSet.contents, 'round-trips NS contents');
  assert.equal(roundtrip.binSet.datatype, original.binSet.datatype, 'round-trips BS datatype');
  assert.deepEqual(roundtrip.binSet.contents, original.binSet.contents, 'round-trips BS contents');

  assert.deepEqual(roundtrip.list, original.list, 'round-trips list attribute');

  assert.deepEqual(roundtrip.map.nested.str, original.map.nested.str, 'round-trips map str attribute');
  assert.deepEqual(roundtrip.map.nested.num, original.map.nested.num, 'round-trips map num attribute');
  assert.deepEqual(roundtrip.map.nested.bin, original.map.nested.bin, 'round-trips map bin attribute');
  assert.deepEqual(roundtrip.map.nested.bool, original.map.nested.bool, 'round-trips map bool attribute');
  assert.deepEqual(roundtrip.map.nested.nothin, original.map.nested.nothin, 'round-trips map null attribute');

  assert.equal(roundtrip.map.nested.strSet.datatype, original.map.nested.strSet.datatype, 'round-trips map SS datatype');
  assert.deepEqual(roundtrip.map.nested.strSet.contents, original.map.nested.strSet.contents, 'round-trips map SS contents');
  assert.equal(roundtrip.map.nested.numSet.datatype, original.map.nested.numSet.datatype, 'round-trips map NS datatype');
  assert.deepEqual(roundtrip.map.nested.numSet.contents, original.map.nested.numSet.contents, 'round-trips map NS contents');
  assert.equal(roundtrip.map.nested.binSet.datatype, original.map.nested.binSet.datatype, 'round-trips map BS datatype');
  assert.deepEqual(roundtrip.map.nested.binSet.contents, original.map.nested.binSet.contents, 'round-trips map BS contents');

  assert.deepEqual(roundtrip.map.nested.list, original.map.nested.list, 'round-trips map list attribute');

  assert.deepEqual(roundtrip.trickyMap.SS, original.trickyMap.SS, 'round-trips a list with SS key');
  assert.deepEqual(roundtrip.trickyMap.BS, original.trickyMap.BS, 'round-trips a list with a BS key');

  assert.equal(roundtrip.trickyMap.B.datatype, original.trickyMap.B.datatype, 'round-trips a SS datatype with BS key');
  assert.deepEqual(roundtrip.trickyMap.B.contents, original.trickyMap.B.contents, 'round-trips a SS contents with BS key');

  assert.end();
});

test('[serialization] deserialize string containing falsy set', function(assert) {
  var str = JSON.stringify({ set: { NS: [0] } });
  var obj = Dyno.deserialize(str);
  assert.deepEqual(obj, {
    set: {
      wrapperName: 'Set',
      type: 'Number',
      values: [0]
    }
  }, 'success');
  assert.end();
});
