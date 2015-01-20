module.exports = function(config) {
    var kinesis = {};

    kinesis.enabled = (config.kinesis !== undefined);

    kinesis.putRecord = function(action, record) {
        var data = {
            action: action,
            table: record.TableName,
            item: record.Item
        };
        var params = {
            StreamName: config.kinesisStream,
            PartitionKey: record.Item[config.kinesisPartitionAttribute].S,
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
