var seedrandom = require('seedrandom');

module.exports.test = {
    'TableName': 'test',
    'AttributeDefinitions': [
        {'AttributeName': 'id', 'AttributeType': 'S'},
        {'AttributeName': 'range', 'AttributeType': 'N'}
    ],
    'KeySchema': [
        {'AttributeName': 'id', 'KeyType': 'HASH'},
        {'AttributeName': 'range', 'KeyType': 'RANGE'}
    ],
    'ProvisionedThroughput': {
        'ReadCapacityUnits': 1,
        'WriteCapacityUnits': 1
    }
};

module.exports.live = {
    'AttributeDefinitions': [
        {'AttributeName': 'id', 'AttributeType': 'S'},
        {'AttributeName': 'range', 'AttributeType': 'N'}
    ],
    'KeySchema': [
        {'AttributeName': 'id', 'KeyType': 'HASH'},
        {'AttributeName': 'range', 'KeyType': 'RANGE'}
    ],
    'ProvisionedThroughput': {
        'ReadCapacityUnits': 5000,
        'WriteCapacityUnits': 5000
    }
};

module.exports.randomItems = function(n, itemsize) {
    var items = [];
    var rng = seedrandom('test');
    var data;
    itemsize = itemsize || Math.round(rng() * 10000);

    for (var i = 0; i < n; i++) {
        data = '';
        for (var k = 0; k < itemsize; k++) {
            data += Math.floor(10 * rng()).toString();
        }

        items.push({
            id: 'id:' + i.toString(),
            range: i,
            data: data
        });
    }
    return items;
};
