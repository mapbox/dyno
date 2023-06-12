## v1.6.1
- Add asynchronous versions of existing methods that support callbacks

## v1.6.0

- Support passing a `costLogger` which will be called with casted `ConsumedCapacity`
- Support reusing DynamoDB Client by passing `dynoInstance` 

## v1.5.2

- update reduceCapacity to support new data shape [#157](https://github.com/mapbox/dyno/pull/157)

## v1.5.1

- updates minimist dependency to v1.2.6 from v1.2.5

## v1.5.0

- drop support for Node 6 & 8
- add support for node 12
- update critical severity dependencies including eslint, underscore, nyc, documentation, coveralls, minimist
- remove .travis.yml tests and cloudformation template, run tests with codebuild
- update @mapbox/dynamodb-test to use the latest dynalite and leveldown packages
- replace `Buffer()` with `Buffer.from()`
- automatically remove `TableId` before createTable to support round-tripping requests from `export -> import`

## v1.4.2

- pin event-stream to `3.3.4`
- use @mapbox/dynamodbtest

## v1.4.0

- updates aws-sdk dependency to `2.x` to help reduce the weight of the aws-sdk in your bundle

## v1.3.0

- updates aws-sdk dependency to v2.7+ from v2.1+


## v1.2.1

- fixes a bug where certain errors were not handled correctly by `.batchWriteAll()`

## v1.2.0

- adds `.batchGetAll()` and `.batchWriteAll()` methods
- adds autopagination to `.query()` and `.scan()` methods via a `Pages` parameter.
- fixes a bug in the cli which prevented export --> import roundtrips

## v1.1.0

- adds `dyno.putStream()`, a writable stream to batch individual records into `BatchWriteItem` requests
- bug fixes in cli

## v1.0.1

- fixes a bug in converting JavaScript arrays to DynamoDB Sets if the first item in the array is falsy (e.g `0`)

## v1.0.0

The overarching goal is to prefer to utilize the aws-sdk commands where possible, inheriting their bug fixes, updates, [and documentation](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/index.html). Some aspects of dyno's API are really nice (e.g. `.getItem(key)` instead of `.getItem({ Key: key })`), but having documentation that we don't have to maintain is arguably better.

### generally speaking

... the big changes are:

- "dyno-style" function arguments, conditions, and options go away. Instead all function arguments are as described for the aws-sdk's [document client](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html).
- dyno navigates the differences between the aws-sdk's [regular client](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html) and [document client](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html) for you behind the scenes.
- with the exception of explicit `stream` functions, none of the function calls fan out into multiple HTTP requests. It doesn't handle pagination, it doesn't split large batch requests into a series of acceptably-sized requests, and it doesn't attempt to retry errors. Configuring when and how errors are retried becomes the client's responsibility using [aws-sdk's mechanisms](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Request.html#retry-event).


### breaking changes

- **client configuration** requires you to provide a `table` name
- **dyno.getItems** renamed to **dyno.batchGetItem**, as a passthrough to aws-sdk.
	- no longer accepts an array of keys and optional `options` object. See [aws-sdk docs for parameters](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#batchGet-property)
	- does not allow you to request more than 100 items per function call
	- does not provide a readable stream of response records
- **dyno.deleteItems** and **dyno.putItems** replaced by **dyno.batchWriteItem**, a passthrough to aws-sdk.
	- no longer accepts an array of items to put / keys to delete and optional `options` object. See [aws-sdk docs for parameters](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#batchWrite-property)
	- does not allow you to write > 25 items or 16 MB per function call
- **dyno.deleteItem** becomes a passthrough to aws-sdk. No longer accepts a key and optional `options` object. See [aws-sdk docs for parameters](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#delete-property)
- **dyno.getItem** becomes a passthrough to aws-sdk. No longer accepts a key and optional `options` object. See [aws-sdk docs for parameters](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#get-property)
- **dyno.putItem** becomes a passthrough to aws-sdk. No longer accepts a record and optional `options` object. See [aws-sdk docs for parameters](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#put-property)
- **dyno.updateItem** becomes a passthrough to aws-sdk. No longer accepts a key, "dyno-style" update definition and optional `options` object. See [aws-sdk docs for parameters](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#update-property)
- **dyno.query** becomes a passthrough to aws-sdk.
	- no longer accepts the "dyno-style" query conditions and optional `options` object. See [aws-sdk docs for parameters](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#query-property)
	- does not paginate through the reponse for you. Pagination can be accomplished manually or via [aws-sdk's functions](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Request.html#eachPage-property).
	- does not provide a readable stream of response records
- **dyno.scan** becomes a passthrough to aws-sdk.
	- no longer accepts an optional `options` object. [aws-sdk docs for parameters](See http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#scan-property)
	- does not paginate through the response for you. Pagination can be accomplished manually or via [aws-sdk's functions](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Request.html#eachPage-property)
	- does not provide a readable stream of response records
- drops **dyno.estimateSize**
- drops all **kinesis-related functionality**

### new functions

- **dyno.queryStream** provides a readable stream of query responses. Parameters passed to the function are identical to [dyno.query](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#query-property), but the function returns a readable stream that will paginate through results as you read from it.
- **dyno.scanStream** provides a readable stream of scan responses. Parameters passed to the function are identical to [dyno.scan](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#scan-property), but the function returns a readable stream that will paginate through results as you read from it.
- **dyno.batchWriteItemRequests** provides an array of [AWS.Request](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Request.html) objects representing BatchWriteItem requests. Parameters passed to the function are identical to [dyno.batchWriteItem](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#batchWrite-property), except that there is no size/count limit to the number or write requests you may provide.
- **dyno.batchGetItemRequests** provides an array of [AWS.Request](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Request.html) objects representing BatchGetItem requests. Parameters passed to the function are identical to [dyno.batchGetItem](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#batchGet-property), except that there is no size/count limit to the number or get requests you may provide.

## v0.17.0

- Return native format (not wire format) for `metas[*].last` (`LastEvaluatedKey`).
- Callers must use native format (not wire format) when passing in `opts.start` (`ExclusiveStartKey`).

## v0.16.0

- Fixed a bug that did not allow the CLI tool to function using EC2 IAM Role-based credentials
- Adds a function to estimate the number of bytes required to store an item
- Allows caller to set concurrency on batch requests

## v0.15.1

- Batch write requests now return any keys that went unprocessed

## v0.15.0

- Add support for [document types (List and Map)][1], [Boolean][2], and [Null][3]
- Native JavaScript arrays will now transform into lists instead of sets
- Add `Dyno.createSet()`, which constructs sets (of number, string, or binary
  type) that transform to the DynamoDB wire format correctly.
- Drop support for using a wire-formatted object as input.
- Add `Dyno.serialize()` and `Dyno.deserialize()` to convert items to/from strings

[1]:http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DataModel.html#DataModel.DataTypes.Document
[2]:http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DataModel.html#DataModel.DataTypes.Boolean
[3]:http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DataModel.html#DataModel.DataTypes.Null
