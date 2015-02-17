process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';

var Dyno = require('..');
var https = require('https');
var AWS = require('aws-sdk');
var s = require('./setup')();
var test = s.test;
var dynalite = {
    start: function(t) {
        s.setup()(t);
    },
    table: function(t) {
        s.setupTable(t);
    },
    stop: function(t) {
        s.teardown(t);
    }
};
var kinesalite = require('kinesalite');
var kinesis = {
    streamname: 'mock-kinesis-stream',
    streamregion: 'us-east-1',

    server: null,

    start: function(t) {
        kinesis.server = kinesalite({ createStreamMs: 50 });
        kinesis.server.listen(7654, function(err) {
            t.ifError(err, 'started kinesalite');
            t.end();
        });
    },

    stream: function(t) {
        var agent = new https.Agent();
        agent.rejectUnauthorized = false;

        var k = new AWS.Kinesis({
            region: kinesis.streamregion,
            accessKeyId: 'fake',
            secretAccessKey: 'fake',
            endpoint: new AWS.Endpoint('https://localhost:7654'),
            httpOptions: { agent: agent }
        });
        k.createStream({
            StreamName: kinesis.streamname,
            ShardCount: 1
        }, function(err, data) {
            t.ifError(err, 'created stream');
            t.end();
        });
    },
    stop: function(t) {
        kinesis.server.close();
        t.end();
    }
};

var dyno = Dyno({
    accessKeyId: 'fake',
    secretAccessKe: 'fake',
    region: 'us-east-1',
    endpoint: new AWS.Endpoint('http://localhost:4567'),
    table: s.tableName,
    kinesisConfig: {
        stream: kinesis.streamname,
        region: kinesis.streamregion,
        key: ['id', 'range']
    }
});

test('start dynalite', dynalite.start);
test('dynalite table', dynalite.table);
test('start kinesis', kinesis.start);
test('kinesis stream', kinesis.stream);
test('write requests', function(t) {
    dyno.putItem({id: 'a', range: 7, data: 'ham'}, function(err) {
        t.ifError(err, 'no error');
        t.end();
    });
});
test('get requests', function(t) {
    dyno.getItem({id: 'a', range: 7}, function(err, item) {
        t.ifError(err, 'no error');
        t.equal(item.data, 'ham', 'got record');
        t.end();
    });
});
test('batch write requests', function(t) {
    dyno.putItems([
        { id: 'b', range: 8, data: 'eggs' },
        { id: 'c', range: 9, data: 'bacon' }
    ], function(err) {
        t.ifError(err, 'no error');
        t.end();
    });
});
test('stop dynalite', dynalite.stop);
test('stop kinesis', kinesis.stop);
