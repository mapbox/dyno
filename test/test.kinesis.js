var test = require('tap').test;
var config = require('../lib/config');
var Kinesis = require('../lib/kinesis');
var Dyno = require('..');

test('config: invalid kinesisConfig', function(t) {
    t.throws(function() {
        config({
            table: 'blah',
            kinesisConfig: {
                region: 'blah',
                key: ['blah']
            }
        });
    }, 'kinesisConfig.stream is required');

    t.throws(function() {
        config({
            table: 'blah',
            kinesisConfig: {
                stream: 'blah',
                key: ['blah']
            }
        });
    }, 'kinesisConfig.region is required');

    t.throws(function() {
        config({
            table: 'blah',
            kinesisConfig: {
                stream: 'blah',
                region: 'blah'
            }
        });
    }, 'kinesisConfig.key is required');

    t.throws(function() {
        config({
            kinesisConfig: {
                stream: 'blah',
                key: ['blah'],
                region: 'blah'
            }
        });
    }, 'kinesisConfig.table is required');

    t.end();
});

test('config: creates kinesis object', function(t) {
    var c = config({
        table: 'fake',
        kinesisConfig: {
            region: 'us-east-1',
            key: ['id', 'range'],
            stream: 'fake'
        }
    });

    t.ok(c.kinesis, 'created');
    t.end();
});

test('kinesis: disabled for read requests', function(t) {
    var c = config({
        region: 'us-east-1',
        table: 'fake',
        kinesisConfig: {
            region: 'us-east-1',
            key: ['id', 'range'],
            stream: 'fake'
        }
    });

    var expectations = {
        query: false,
        scan: false,
        updateItem: true,
        putItem: true,
        getItem: false,
        deleteItem: true,
        batchGetItem: false,
        batchWriteItem: true,
        describeTable: false
    };

    for (var request in expectations) {
        t.equal(
            Kinesis(request, c).enabled,
            expectations[request],
            request + ' was' + (expectations[request] ? ' ' : ' not ') + 'enabled'
        );
    }

    t.end();
});

test('kinesis params', function(t) {
    var kinesis = Kinesis('putItem', {
        dynamo: { config: { region: 'us-east-1' } },
        kinesisConfig: { key: ['id'] }
    });

    function clean(params) {
        params.Data = params.Data.replace(/"eventID":"(.+?)",/, '"eventID":"[EVENTID]",');
        return params;
    }

    var found = kinesis.params('updateItem', {
        Key: { id: { S: 'the-id' } },
        TableName: 'fake',
        AttributeUpdates: { data: { Action: 'PUT', value: { BOOL: true } } }
    });

    t.deepEqual(clean(found), {
        Data: JSON.stringify({
            awsRegion: 'us-east-1',
            eventID: '[EVENTID]',
            eventName: 'MODIFY',
            eventSource: 'aws:dynamodb',
            eventVersion: '1.0',
            dynamodb: {
                Keys: { id: { S: 'the-id' } },
                SequenceNumber: '0',
                SizeBytes: 0,
                StreamViewType: 'KEYS_ONLY'
            }
        }),
        PartitionKey: JSON.stringify({ id: { S: 'the-id' } })
    }, 'expected updateItem params');

    found = kinesis.params('putItem', {
        TableName: 'fake',
        Item: { id: { S: 'the-id' }, data: { S: 'ham' } }
    });

    t.deepEqual(clean(found), {
        Data: JSON.stringify({
            awsRegion: 'us-east-1',
            eventID: '[EVENTID]',
            eventName: 'INSERT',
            eventSource: 'aws:dynamodb',
            eventVersion: '1.0',
            dynamodb: {
                Keys: { id: { S: 'the-id' } },
                SequenceNumber: '0',
                SizeBytes: 0,
                StreamViewType: 'KEYS_ONLY'
            }
        }),
        PartitionKey: JSON.stringify({ id: { S: 'the-id' } })
    }, 'expected putItem params');

    found = kinesis.params('deleteItem', {
        TableName: 'fake',
        Key: { id: { S: 'the-id' } }
    });

    t.deepEqual(clean(found), {
        Data: JSON.stringify({
            awsRegion: 'us-east-1',
            eventID: '[EVENTID]',
            eventName: 'REMOVE',
            eventSource: 'aws:dynamodb',
            eventVersion: '1.0',
            dynamodb: {
                Keys: { id: { S: 'the-id' } },
                SequenceNumber: '0',
                SizeBytes: 0,
                StreamViewType: 'KEYS_ONLY'
            }
        }),
        PartitionKey: JSON.stringify({ id: { S: 'the-id' } })
    }, 'expected deleteItem params');

    t.end();
});

test('kinesis put: disabled', function(t) {
    var config = {
        kinesis: {
            putRecord: function(params, callback) {
                t.fail('should not putRecord');
                callback();
            },
            putRecords: function(params, callback) {
                t.fail('should not putRecords');
                callback();
            }
        },
        dynamo: { config: { region: 'us-east-1' } },
        table: 'dynamo-table',
        kinesisConfig: {
            stream: 'stream-id',
            key: ['id', 'range']
        }
    };

    var kinesis = Kinesis('getItem', config);
    t.notOk(kinesis.enabled, 'disabled kinesis');
    kinesis.put({
        TableName: config.table,
        Item: { id: { S: 'the-id' }, range: { S: 'ham' } }
    }, function(err, data) {
        t.ifError(err, 'no error');
        t.notOk(data, 'no response');
        t.end();
    });
});

test('kinesis put: updateItem', function(t) {
    function clean(params) {
        params.Data = params.Data.replace(/"eventID":"(.+?)",/, '"eventID":"[EVENTID]",');
        return params;
    }

    var requests = 0;
    var config = {
        kinesis: {
            putRecord: function(params, callback) {
                requests++;
                t.ok(requests < 2, 'only one putRecord request made');
                t.deepEqual(clean(params), {
                    StreamName: 'stream-id',
                    Data: JSON.stringify({
                        awsRegion: 'us-east-1',
                        eventID: '[EVENTID]',
                        eventName: 'MODIFY',
                        eventSource: 'aws:dynamodb',
                        eventVersion: '1.0',
                        dynamodb: {
                            Keys: { id: { S: 'the-id' }, range: { S: 'ham' } },
                            SequenceNumber: '0',
                            SizeBytes: 0,
                            StreamViewType: 'KEYS_ONLY'
                        }
                    }),
                    PartitionKey: JSON.stringify({ id: { S: 'the-id' }, range: { S: 'ham' } })
                }, 'expected putRecord params');
                callback();
            },
            putRecords: function(params, callback) {
                t.fail('should not putRecords');
                callback();
            }
        },
        dynamo: { config: { region: 'us-east-1' } },
        table: 'dynamo-table',
        kinesisConfig: {
            stream: 'stream-id',
            key: ['id', 'range']
        }
    };

    var kinesis = Kinesis('updateItem', config);

    kinesis.put({
        Key: { id: { S: 'the-id' }, range: { S: 'ham' } },
        TableName: config.table,
        AttributeUpdates: { data: { Action: 'PUT', value: { BOOL: true } } }
    }, function(err, data) {
        t.ifError(err, 'no error');
        t.end();
    });
});

test('kinesis put: putItem', function(t) {
    function clean(params) {
        params.Data = params.Data.replace(/"eventID":"(.+?)",/, '"eventID":"[EVENTID]",');
        return params;
    }

    var requests = 0;
    var config = {
        kinesis: {
            putRecord: function(params, callback) {
                requests++;
                t.ok(requests < 2, 'only one putRecord request made');
                t.deepEqual(clean(params), {
                    StreamName: 'stream-id',
                    Data: JSON.stringify({
                        awsRegion: 'us-east-1',
                        eventID: '[EVENTID]',
                        eventName: 'INSERT',
                        eventSource: 'aws:dynamodb',
                        eventVersion: '1.0',
                        dynamodb: {
                            Keys: { id: { S: 'the-id' }, range: { S: 'ham' } },
                            SequenceNumber: '0',
                            SizeBytes: 0,
                            StreamViewType: 'KEYS_ONLY'
                        }
                    }),
                    PartitionKey: JSON.stringify({ id: { S: 'the-id' }, range: { S: 'ham' } })
                }, 'expected putRecord params');
                callback();
            },
            putRecords: function(params, callback) {
                t.fail('should not putRecords');
                callback();
            }
        },
        dynamo: { config: { region: 'us-east-1' } },
        table: 'dynamo-table',
        kinesisConfig: {
            stream: 'stream-id',
            key: ['id', 'range']
        }
    };

    var kinesis = Kinesis('putItem', config);

    kinesis.put({
        TableName: config.table,
        Item: { id: { S: 'the-id' }, range: { S: 'ham' } }
    }, function(err, data) {
        t.ifError(err, 'no error');
        t.end();
    });
});

test('kinesis put: deleteItem', function(t) {
    function clean(params) {
        params.Data = params.Data.replace(/"eventID":"(.+?)",/, '"eventID":"[EVENTID]",');
        return params;
    }

    var requests = 0;
    var config = {
        kinesis: {
            putRecord: function(params, callback) {
                requests++;
                t.ok(requests < 2, 'only one putRecord request made');
                t.deepEqual(clean(params), {
                    StreamName: 'stream-id',
                    Data: JSON.stringify({
                        awsRegion: 'us-east-1',
                        eventID: '[EVENTID]',
                        eventName: 'REMOVE',
                        eventSource: 'aws:dynamodb',
                        eventVersion: '1.0',
                        dynamodb: {
                            Keys: { id: { S: 'the-id' } },
                            SequenceNumber: '0',
                            SizeBytes: 0,
                            StreamViewType: 'KEYS_ONLY'
                        }
                    }),
                    PartitionKey: JSON.stringify({ id: { S: 'the-id' } })
                }, 'expected putRecord params');
                callback();
            },
            putRecords: function(params, callback) {
                t.fail('should not putRecords');
                callback();
            }
        },
        dynamo: { config: { region: 'us-east-1' } },
        table: 'dynamo-table',
        kinesisConfig: {
            stream: 'stream-id',
            key: ['id', 'range']
        }
    };

    var kinesis = Kinesis('deleteItem', config);

    kinesis.put({
        TableName: config.table,
        Key: { id: { S: 'the-id' } }
    }, function(err, data) {
        t.ifError(err, 'no error');
        t.end();
    });
});

test('kinesis put: batchWriteItem', function(t) {
    function clean(params) {
        params.Records = params.Records.map(function(record) {
            record.Data = record.Data.replace(/"eventID":"(.+?)",/, '"eventID":"[EVENTID]",');
            return record;
        });
        return params;
    }

    var requests = 0;
    var config = {
        kinesis: {
            putRecord: function(params, callback) {
                t.fail('should not putRecord');
                callback();
            },
            putRecords: function(params, callback) {
                requests++;
                t.ok(requests < 2, 'only one putRecords request made');
                t.deepEqual(clean(params), {
                    StreamName: 'stream-id',
                    Records: [
                        {
                            Data: JSON.stringify({
                                awsRegion: 'us-east-1',
                                eventID: '[EVENTID]',
                                eventName: 'INSERT',
                                eventSource: 'aws:dynamodb',
                                eventVersion: '1.0',
                                dynamodb: {
                                    Keys: { id: { S: 'the-first-id' }, range: { S: 'ham' } },
                                    SequenceNumber: '0',
                                    SizeBytes: 0,
                                    StreamViewType: 'KEYS_ONLY'
                                }
                            }),
                            PartitionKey: JSON.stringify({ id: { S: 'the-first-id' }, range: { S: 'ham' } })
                        },
                        {
                            Data: JSON.stringify({
                                awsRegion: 'us-east-1',
                                eventID: '[EVENTID]',
                                eventName: 'REMOVE',
                                eventSource: 'aws:dynamodb',
                                eventVersion: '1.0',
                                dynamodb: {
                                    Keys: { id: { S: 'the-first-id' }, range: { S: 'ham' } },
                                    SequenceNumber: '0',
                                    SizeBytes: 0,
                                    StreamViewType: 'KEYS_ONLY'
                                }
                            }),
                            PartitionKey: JSON.stringify({ id: { S: 'the-first-id' }, range: { S: 'ham' } })
                        },
                        {
                            Data: JSON.stringify({
                                awsRegion: 'us-east-1',
                                eventID: '[EVENTID]',
                                eventName: 'INSERT',
                                eventSource: 'aws:dynamodb',
                                eventVersion: '1.0',
                                dynamodb: {
                                    Keys: { id: { S: 'the-id' }, range: { S: 'ham and eggs' } },
                                    SequenceNumber: '0',
                                    SizeBytes: 0,
                                    StreamViewType: 'KEYS_ONLY'
                                }
                            }),
                            PartitionKey: JSON.stringify({ id: { S: 'the-id' }, range: { S: 'ham and eggs' } })
                        }
                    ]
                }, 'expected putRecords params');
                callback();
            }
        },
        dynamo: { config: { region: 'us-east-1' } },
        table: 'dynamo-table',
        kinesisConfig: {
            stream: 'stream-id',
            key: ['id', 'range']
        }
    };

    var kinesis = Kinesis('batchWriteItem', config);

    kinesis.put({
        RequestItems: {
            'dynamo-table': [
                { PutRequest: { Item: { id: { S: 'the-first-id' }, range: { S: 'ham' } } } },
                { DeleteRequest: { Key: { id: { S: 'the-first-id' }, range: { S: 'ham' } } } },
                { PutRequest: { Item: { id: { S: 'the-id' }, range: { S: 'ham and eggs' } } } }
            ]
        }
    }, function(err, data) {
        t.ifError(err, 'no error');
        t.end();
    });
});
