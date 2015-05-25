var test = require('tape');
var Dyno = require('..');
var https = require('https');
var AWS = require('aws-sdk');

test('isolates credentials', function(assert) {
    process.env.AWS_ACCESS_KEY_ID = '';
    process.env.AWS_SECRET_ACCESS_KEY = '';
    process.env.AWS_SESSION_TOKEN = '';
    AWS.config.update({
        accessKeyId: 'default',
        secretAccessKey: 'default',
        sessionToken: 'default',
        endpoint: 'default'
    });

    var dynoA = Dyno({
        region: 'fake',
        accessKeyId: 'fake',
        secretAccessKey: 'fake',
        endpoint: 'fake'
    });

    var dynoB = Dyno({
        region: 'us-east-1'
    });

    assert.notEqual(dynoA.config.region, dynoB.config.region, 'isolated regions');
    if (dynoB.config.credentials) {
        assert.notEqual(dynoA.config.credentials.accessKeyId, dynoB.config.credentials.accessKeyId, 'isolated creds');
    }
    assert.deepEqual(dynoB.config.credentials, { accessKeyId: 'default', expireTime: null, expired: false, sessionToken: 'default' }, 'no credentials assigned');
    assert.end();
});

test('allows httpOptions', function(assert) {
    var dyno = Dyno({
        region: 'us-east-1',
        httpOptions: {
            agent: new https.Agent({ maxSockets: 16 })
        }
    });

    assert.equal(dyno.config.httpOptions.agent.maxSockets, 16, 'sets httpOptions');
    assert.end();
});
