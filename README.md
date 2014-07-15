## Dyno

The aws-sdk dynamo client is very close to the API, dyno tries to help reduce the
amount of repetitive code needed to interact with dynamodb.

First it guesses types. Dynamo is very specific about its types:

```
{key:{S: 'value'}}
```

in dyno can be written like:

```
{key:'value'}
```

When dyno doesn't do anything to improve a command, it simply passes it through to
[aws-sdk dynamodb client](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html).

This is the case right now with commands like `scan` and `query`


#### Installing

```
 npm install dyno -S
```

### Usage


##### Setup

```
var dyno = module.exports.dyno = require('dyno')({
    awsKey: 'XXX',
    awsSecret: 'XXX',
    region: 'us-east-1',
    table: 'test'
});

```

##### putItem

```
var item = {id: 'yo', range: 5};
dyno.putItem(item, function(err, resp){});

```

Set the table name per command:

```
var item = {id: 'yo', range: 5};
dyno.putItem(item, {table:'myothertablename'}, function(err, resp){});

```

##### updateItem

```
var key = {id: 'yo', range:5};
var item = {put:{a: 'oh hai'}, add:{count: 1}};

dyno.updateItem(key, item, function(err, resp){});

```
