var types = require('./types');

module.exports = function(config) {
    var kinesis = {};

    kinesis.enabled = (config.kinesis !== undefined);

    kinesis.putRecord = function(action, record) {
        var primaryKey = {};

        // figure out primary key from describe table request
        // cache that forever (until restart) because it doesnt change.

        if(action == 'updateItem') {
            primaryKey = record.Key;
        } else if(action == 'putItem') {
            primaryKey[config.kinesisPrimaryKey] = types.typesFromDynamo(
                record.Item[config.kinesisPrimaryKey])[0];
        } else {
            // TODO add deleteItem, batchWriteItem
            throw new Error('Unsupported action: ' + action);
        }

        console.log('primary key:', primaryKey);
        var data = {
            table: record.TableName,
            key: primaryKey
        };
        var params = {
            StreamName: config.kinesisStream,
            PartitionKey: JSON.stringify(primaryKey),
            Data: JSON.stringify(data),
        };
        console.log('kinesis params:', params);

        config.kinesis.putRecord(params, function(err, data) {
            console.log('kinesis err:', err);
            console.log('kinesis data:', data);
            // TOOD error handling
        });
    }

    return kinesis;
}
