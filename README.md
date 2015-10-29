## Dyno

[![Build Status](https://travis-ci.org/mapbox/dyno.svg?branch=master)](https://travis-ci.org/mapbox/dyno)

Dyno provides a DynamoDB client that adds additional functionality beyond what is provided by the [aws-sdk-js](https://github.com/aws/aws-sdk-js).

## Overview

### Native JavaScript objects

Dyno operates as an extension to the [aws-sdk's DocumentClient](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html). This means that instead of interacting with typed JavaScript objects representing your DynamoDB records, you can use more "native" objects. For example, the following represents a typed object that could be stored in DynamoDB:

```js
{
  id: { S: 'my-record' },
  numbers: { L: [{ N: '1' }, { N: '2' }, { N: '3' }] },
  data: { B: new Buffer('Hello World!') },
  version: { N: '5' },
}
```

Using Dyno, you can represent the same data in a "native" object:

```js
{
  id: 'my-record',
  numbers: [1, 2, 3],
  data: new Buffer('Hello World!'),
  version: 5
}
```

### Streaming query and scan results

A large query or scan operation may require multiple HTTP requests in order to retrieve all the desired data. Dyno provides functions that allow you to read those data from a native Node.js Readable Stream. Behind-the-scenes, Dyno manages making paginated requests to DynamoDB for you, and emits objects representing each of the records in the aggregated response.

### Chunked batch getItem and writeItem requests

`BatchGetItem` and `BatchWriteItem` requests come with limits as to how much data you can ask for in a single HTTP request. Dyno functions allow you to present the entire set of batch requests that you wish to make. Dyno breaks your set up into an array of request objects each of which is within the limits of a single acceptable request. You can then send each of these requests and handle each of their responses individually.

### Multi-table client

For situations where you may wish to write to one database and read from another. Dyno allows you to configure a client with parameters for two different tables, then routes your individual requests to the appropriate one.

### De/serialization

Dyno exposes functions capable of serializing and deserializing native JavaScript objects representing DynamoDB records to and from wire-formatted strings acceptable as the body of any DynamoDB HTTP request.

### Command-line interface

Dyno provides a CLI tool capable of reading and writing individual records, scanning, exporting and importing data into a DynamoDB table.
