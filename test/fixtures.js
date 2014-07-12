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
