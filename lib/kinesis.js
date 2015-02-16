var types = require('./types');
var crypto = require('crypto');

var writeRequests = [
    'updateItem',
    'putItem',
    'deleteItem',
    'batchWriteItem'
];

function payload(region, key, action) {
    var eventName;
    if (action === 'updateItem') eventName = 'MODIFY';
    if (action === 'putItem') eventName = 'INSERT';
    if (action === 'deleteItem') eventName = 'REMOVE';

    var data = {
        awsRegion: region,
        eventId: crypto.randomBytes(16).toString('hex'),
        eventName: eventName,
        eventSource: 'aws:dynamodb',
        eventVersion: '1.0',
        dynamodb: {
            Keys: key,
            SequenceNumber: '0',
            SizeBytes: 0,
            StreamViewType: 'KEYS_ONLY'
        }
    };

    return JSON.stringify(data);
}

module.exports = function(requestType, config) {
    var kinesis = {};

    kinesis.enabled = config.kinesis && writeRequests.indexOf(requestType) !== -1;

    // Takes request parameters and build putRecord parameters
    kinesis.params = function(action, request) {
        var key = {};
        if (action === 'updateItem') key = request.Key;
        if (action === 'deleteItem') key = request.Key;
        if (action === 'putItem') {
            config.kinesisConfig.key.forEach(function(keyFieldName) {
                key[keyFieldName] = types.typesFromDynamo(
                    request.Item[keyFieldName]
                );
            });
        }

        return {
            Data: payload(config.dynamo.config.region, key, action),
            PartitionKey: JSON.stringify(key)
        };
    };

    // Perform a put to a kinesis stream
    kinesis.put = function(request) {
        if (!kinesis.enabled) return;

        var putParams = {
            StreamName: config.kinesisConfig.stream
        };

        var kinesisRequest;

        if (requestType === 'batchWriteItem') {
            kinesisRequest = 'putRecords';
            putParams.Records = request.RequestItems[config.table].map(function(req) {
                var action = req.PutRequest ? 'putItem' : 'deleteItem';
                var params = req.PutRequest || req.DeleteRequest;
                return kinesis.params(action, params);
            });
        } else {
            kinesisRequest = 'putRecord';
            putParams.extend(kinesis.params(request));
        }

        config.kinesis[kinesisRequest](putParams, function(err, data) {});
    };

    return kinesis;
};
