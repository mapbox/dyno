# client

A dyno client which extends the [aws-sdk's DocumentClient](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html).

## batchGetAll

Break a large batch of get operations into a set of requests that are intended
to be sent concurrently.

**Parameters**

-   `params` **object** unbounded batchGetItem request parameters. See [DocumentClient.batchGet](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#batchGet-property) for details.
- `maxRetries` **[number]** number of times to retry sending unprocessed items.

Returns **CompleteRequestSet**

## batchGetItem

Perform a batch of get operations. Passthrough to [DocumentClient.batchGet](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#batchGet-property).

**Parameters**

-   `params` **object** request parameters. See [DocumentClient.batchGet](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#batchGet-property) for details.
-   `callback` **[function]** a function to handle the response. See [DocumentClient.batchGet](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#batchGet-property) for details.

Returns **Request**

## batchGetItemRequests

Break a large batch of get operations into a set of requests that can be
sent individually or concurrently.

**Parameters**

-   `params` **object** unbounded batchGetItem request parameters. See [DocumentClient.batchGet](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#batchGet-property) for details.

Returns **RequestSet**

## batchWriteAll

Break a large batch of write operations into a set of requests that are intended
to be sent concurrently.

**Parameters**

-   `params` **object** unbounded batchWriteItem request parameters. See [DocumentClient.batchWrite](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#batchWrite-property) for details.
- `maxRetries` **[number]** number of times to retry sending unprocessed items.

Returns **CompleteRequestSet**

## batchWriteItem

Perform a batch of write operations. Passthrough to [DocumentClient.batchWrite](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#batchWrite-property).

**Parameters**

-   `params` **object** request parameters. See [DocumentClient.batchWrite](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#batchWrite-property) for details.
-   `callback` **[function]** a function to handle the response. See [DocumentClient.batchWrite](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#batchWrite-property) for details.

Returns **Request**

## batchWriteItemRequests

Break a large batch of write operations into a set of requests that can be
sent individually or concurrently.

**Parameters**

-   `params` **object** unbounded batchWriteItem request parameters. See [DocumentClient.batchWrite](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#batchWrite-property) for details.

Returns **RequestSet**

## createTable

Create a table. Passthrough to [DynamoDB.createTable](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#createTable-property),
except the function polls DynamoDB until the table is ready to accept
reads and writes, at which point the callback function is called.

**Parameters**

-   `params` **object** request parameters. See [DynamoDB.createTable](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#createTable-property) for details.
-   `callback` **[function]** a function to handle the response. See [DynamoDB.createTable](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#createTable-property) for details.

Returns **Request**

## deleteItem

Delete a single record. Passthrough to [DocumentClient.delete](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#delete-property).

**Parameters**

-   `params` **object** request parameters. See [DocumentClient.delete](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#delete-property) for details.
-   `callback` **[function]** a function to handle the response. See [DocumentClient.delete](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#delete-property) for details.

Returns **Request**

## deleteTable

Delete a table. Passthrough to [DynamoDB.deleteTable](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#deleteTable-property),
except the function polls DynamoDB until the table is ready to accept
reads and writes, at which point the callback function is called.

**Parameters**

-   `params` **object** request parameters. See [DynamoDB.deleteTable](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#deleteTable-property) for details.
-   `callback` **[function]** a function to handle the response. See [DynamoDB.deleteTable](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#deleteTable-property) for details.

Returns **Request**

## describeTable

Get table information. Passthrough to [DynamoDB.describeTable](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#describTable-property).

**Parameters**

-   `params` **object** request parameters. See [DynamoDB.describeTable](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#describeTable-property) for details.
-   `callback` **[function]** a function to handle the response. See [DynamoDB.describeTable](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#describeTable-property) for details.

Returns **Request**

## getItem

Get a single record. Passthrough to [DocumentClient.get](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#get-property).

**Parameters**

-   `params` **object** request parameters. See [DocumentClient.get](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#get-property) for details.
-   `callback` **[function]** a function to handle the response. See [DocumentClient.get](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#get-property) for details.

Returns **Request**

## listTables

List the tables available in a given region. Passthrough to [DynamoDB.listTables](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#listTables-property).

**Parameters**

-   `params` **object** request parameters. See [DynamoDB.listTables](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#listTables-property) for details.
-   `callback` **[function]** a function to handle the response. See [DynamoDB.listTables](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#listTables-property) for details.

Returns **Request**

## putItem

Put a single record. Passthrough to [DocumentClient.put](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#put-property).

**Parameters**

-   `params` **object** request parameters. See [DocumentClient.put](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#put-property) for details.
-   `callback` **[function]** a function to handle the response. See [DocumentClient.put](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#put-property) for details.

Returns **Request**

## query

Query a table or secondary index. Passthrough to [DocumentClient.query](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#query-property).

**Parameters**

-   `params` **object** request parameters. See [DocumentClient.query](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#query-property) for details.
    -   `params.Pages` **[number]** maximum number of pages of query results to request. Set to `Infinity` to return all available data. (optional, default `1`)
-   `callback` **[function]** a function to handle the response. See [DocumentClient.query](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#query-property) for details.

Returns **** a Request if not paginating, or a ReadableStream if multiple pages were requested

## queryStream

Provide the results of a query as a [Readable Stream](https://nodejs.org/api/stream.html#stream_class_stream_readable).
This function will paginate through query responses, making HTTP requests
as the caller reads from the stream.

**Parameters**

-   `params` **object** query request parameters. See [DocumentClient.query](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#query-property) for details.

Returns **ReadableStream**

## scan

Scan a table or secondary index. Passthrough to [DocumentClient.scan](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#scan-property).

**Parameters**

-   `params` **object** request parameters. See [DocumentClient.scan](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#scan-property) for details.
    -   `params.Pages` **[number]** maximum number of pages of scan results to request. Set to `Infinity` to return all available data. (optional, default `1`)
-   `callback` **[function]** a function to handle the response. See [DocumentClient.scan](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#scan-property) for details.

Returns **** a Request if not paginating, or a ReadableStream if multiple pages were requested

## scanStream

Provide the results of a scan as a [Readable Stream](https://nodejs.org/api/stream.html#stream_class_stream_readable).
This function will paginate through query responses, making HTTP requests
as the caller reads from the stream.

**Parameters**

-   `params` **object** scan request parameters. See [DocumentClient.scan](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#scan-property) for details.

Returns **ReadableStream**

## updateItem

Update a single record. Passthrough to [DocumentClient.update](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#update-property).

**Parameters**

-   `params` **object** request parameters. See [DocumentClient.update](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#update-property) for details.
-   `callback` **[function]** a function to handle the response. See [DocumentClient.update](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#update-property) for details.

Returns **Request**

# CompleteRequestSet

An array of [AWS.Requests](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Request.html)

## sendAll

Send all the requests in a set, optionally specifying concurrency. This function
will retry unprocessed items and return a single response body aggregated from
results of all the individual requests.

The callback function will be passed a single error object if any occurred, and the
aggregated response body. If all requests encountered an error, the second argument
will be null. Otherwise the callback may be provided with an error as well as
the outcome from successful requests.

**Parameters**

-   `concurrency` **[number]** the concurrency with which to make requests.
    Default value is `1`.
-   `callback` **function** a function to handle the response.

# Dyno

Creates a dyno client. You must provide a table name and the region where the
table resides. Where applicable, dyno will use this table as the default in
your requests. However you can override this when constructing any individual
request.

If you do not explicitly pass credentials when creating a dyno client, the
aws-sdk will look for credentials in a variety of places. See [the configuration guide](http://docs.aws.amazon.com/AWSJavaScriptSDK/guide/node-configuring.html)
for details.

**Parameters**

-   `options` **object** configuration parameters
    -   `options.table` **string** the name of the table to interact with by default
    -   `options.region` **string** the region in which the default table resides
    -   `options.endpoint` **[string]** the dynamodb endpoint url
    -   `options.httpOptions` **[object]** httpOptions to provide the aws-sdk client. See [constructor docs for details](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#constructor-property).
    -   `options.accessKeyId` **[string]** credentials for the client to utilize
    -   `options.secretAccessKey` **[string]** credentials for the client to utilize
    -   `options.sessionToken` **[string]** credentials for the client to utilize
    -   `options.credentials` **[AWS.Credentials]** [AWS.Credentials](https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Credentials.html) object for the client to utilize
    -   `options.logger` **[object]** a writable stream for detailed logging from the aws-sdk. See [constructor docs for details](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#constructor-property).
    -   `options.maxRetries` **[number]** number of times to retry on retryable errors. See [constructor docs for details](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#constructor-property).

**Examples**

```javascript
var Dyno = require('dyno');
var dyno = Dyno({
  table: 'my-table',
  region: 'us-east-1'
});
```

Returns **client** a dyno client

## createSet

Create a DynamoDB set. When writing records to DynamoDB, arrays are interpretted
as `List` type attributes. Use this function to utilize a `Set` instead.

**Parameters**

-   `list` **array** an array of strings, numbers, or buffers to store in
    DynamoDB as a set

**Examples**

```javascript
// This record will store the `data` attribute as a `List` in DynamoDB
var asList = {
 id: 'my-record',
 data: [1, 2, 3]
};

// This record will store the `data` attribute as a `NumberSet` in DynamoDB
var asNumberSet = {
 id: 'my-record',
 data: Dyno.createSet([1, 2, 3]);
};
```

Returns **object** a DynamoDB set

## deserialize

Convert a wire-formatted string into a JavaScript object

**Parameters**

-   `str` **string** the serialized representation of a DynamoDB record

**Examples**

```javascript
var str = '{"id":{"S":"my-record"},"version":{"N":"2"},"data":{"B":"SGVsbG8gV29ybGQh"}}';
console.log(Dyno.deserialize(str));
// {
//   id: 'my-record',
//   version: 2,
//   data: <Buffer 48 65 6c 6c 6f 20 57 6f 72 6c 64 21>
// }
```

Returns **object** a JavaScript object representing the record

## multi

Provides a dyno client capable of reading from one table and writing to another.

**Parameters**

-   `readOptions` **object** configuration parameters for the read table.
-   `writeOptions` **object** configuration parameters for the write table.

Returns **dyno** a dyno client.

## serialize

Convert a JavaScript object into a wire-formatted string that can be sent to
DynamoDB in an HTTP request.

**Parameters**

-   `item` **object** a JavaScript object representing a DynamoDB record

**Examples**

```javascript
var item = {
 id: 'my-record',
 version: 2,
 data: new Buffer('Hello World!')
};
console.log(Dyno.serialize(item));
// {"id":{"S":"my-record"},"version":{"N":"2"},"data":{"B":"SGVsbG8gV29ybGQh"}}
```

Returns **string** the serialized representation of the record

# putStream

Creates a [Writable stream](https://nodejs.org/api/stream.html#stream_class_stream_writable).
Writing individual records to the stream will aggregate them into sets of
25 items and submit them as `BatchWriteItem` requests.

**Parameters**

-   `options` **object** stream options. See [Writable stream documentation](https://nodejs.org/api/stream.html#stream_class_stream_writable_1)
    for available options. The stream **will always** set `objectMode: true` for you.
    -   `options.concurrency` **[number]** set the maximum desired concurrency for
        outgoing BatchWriteItem requests. (optional, default `1`)
    -   `options.retry` **[function]** sets the provided function as a [retry event listener](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Request.html#retry-event)
        on outgoing BatchWriteItem requests.

Returns **** a [Writable stream](https://nodejs.org/api/stream.html#stream_class_stream_writable)

# ReadableStream

A Node.js Readable Stream. See [node.js documentation for details](https://nodejs.org/api/stream.html#stream_class_stream_readable).
This is an `objectMode = true` stream, where each object emitted is a single
DynamoDB record.

**Properties**

-   `ConsumedCapacity` **object** once the stream has begun making requests,
    the `ConsumedCapacity` parameter will report the total capacity consumed by
    the aggregate of all requests that have completed. This property will only
    be present if the `ReturnConsumedCapacity` parameter was set on the initial
    request.

# reduceCapacity

Reduce two sets of consumed capacity metrics into a single object

**Parameters**

-   `existing` **object** capacity. This object will be updated.
-   `new` **object** capacity object to be added to the existing object.
-   `incoming`

# Request

An object representing an aws-sdk request. See [aws-sdk's documentation for details](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Request.html)

# RequestSet

An array of [AWS.Requests](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Request.html)

## sendAll

Send all the requests in a set, optionally specifying concurrency. The
emphasis is on making it transparent to the caller the exact outcome of each
request in the set.

The callback function will be passed arguments in this order:

-   error: set to null if no errors occurred, otherwise an array of errors with
    indexes that correspond to the indexes of the original request set
-   responses: always an array of responses equal with indexes corresponding
    to the original request set. If a particular request encountered an error,
    that index in the `responses` array will be set to `null`.
-   unprocessed: set to null if no unprocessed results were detected, otherwise
    a new set of requests with its own .sendAll function bound to it. Again,
    indexes correspond to those in the original request set.

**Parameters**

-   `concurrency` **[number]** the concurrency with which to make requests.
    Default value is `1`.
-   `callback` **function** a function to handle the response.
