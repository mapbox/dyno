var test = require('tape');
var Dyno = require('..');

test('isolates credentials', function(assert) {
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
    assert.notOk(dynoB.config.credentials, 'no credentials assigned');
    assert.end();
});
