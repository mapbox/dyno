var AWS = require('aws-sdk');
var DynamoDBSet = require('aws-sdk/lib/dynamodb/set');
var _ = require('underscore');

module.exports = Dyno;

/**
 * Creates a dyno client. You must provide a table name and the region where the
 * table resides. Where applicable, dyno will use this table as the default in
 * your requests. However you can override this when constructing any individual
 * request.
 *
 * If you do not explicitly pass credentials when creating a dyno client, the
 * aws-sdk will look for credentials in a variety of places. See [the configuration guide](http://docs.aws.amazon.com/AWSJavaScriptSDK/guide/node-configuring.html)
 * for details.
 *
 * @param {object} options - configuration parameters
 * @param {string} options.table - the name of the table to interact with by default
 * @param {string} options.region - the region in which the default table resides
 * @param {string} [options.endpoint] - the dynamodb endpoint url
 * @param {object} [options.httpOptions] - httpOptions to provide the aws-sdk client. See [constructor docs for details](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#constructor-property).
 * @param {string} [options.accessKeyId] - credentials for the client to utilize
 * @param {string} [options.secretAccessKey] - credentials for the client to utilize
 * @param {string} [options.sessionToken] - credentials for the client to utilize
 * @param {object} [options.logger] - a writable stream for detailed logging from the aws-sdk. See [constructor docs for details](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#constructor-property).
 * @param {number} [options.maxRetries] - number of times to retry on retryable errors. See [constructor docs for details](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#constructor-property).
 * @returns {client} a dyno client
 * @example
 * var Dyno = require('dyno');
 * var dyno = Dyno({
 *   table: 'my-table',
 *   region: 'us-east-1'
 * });
 */
function Dyno(options) {

  /**
   * A dyno client which extends the [aws-sdk's DocumentClient](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html).
   *
   * @name client
   */

  /**
   * An object representing an aws-sdk request. See [aws-sdk's documentation for details](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Request.html)
   *
   * @name Request
   */

  /**
   * A Node.js Readable Stream. See [node.js documentation for details](https://nodejs.org/api/stream.html#stream_class_stream_readable).
   * This is an `objectMode = true` stream, where each object emitted is a single
   * DynamoDB record.
   *
   * @name ReadableStream
   * @property {object} ConsumedCapacity - once the stream has begun making requests,
   * the `ConsumedCapacity` parameter will report the total capacity consumed by
   * the aggregate of all requests that have completed. This property will only
   * be present if the `ReturnConsumedCapacity` parameter was set on the initial
   * request.
   */

  if (!options.table) throw new Error('table is required'); // Demand table be specified
  if (!options.region) throw new Error('region is required');

  var config = {
    region: options.region,
    endpoint: options.endpoint,
    params: { TableName: options.table }, // Sets `TableName` in every request
    httpOptions: options.httpOptions || { timeout: 5000 }, // Default appears to be 2 min
    accessKeyId: options.accessKeyId,
    secretAccessKey: options.secretAccessKey,
    sessionToken: options.sessionToken,
    logger: options.logger,
    maxRetries: options.maxRetries
  };

  var client = new AWS.DynamoDB(config);
  var docClient = new AWS.DynamoDB.DocumentClient({ service: client });
  var tableFreeClient = new AWS.DynamoDB(_(config).omit('params')); // no TableName in batch requests
  var tableFreeDocClient = new AWS.DynamoDB.DocumentClient({ service: tableFreeClient });

  // Straight-up inherit several functions from aws-sdk so we can also inherit docs and tests
  var nativeFunctions = {
    /**
     * List the tables available in a given region. Passthrough to [DynamoDB.listTables](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#listTables-property).
     *
     * @instance
     * @memberof client
     * @param {object} params - request parameters. See [DynamoDB.listTables](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#listTables-property) for details.
     * @param {function} [callback] - a function to handle the response. See [DynamoDB.listTables](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#listTables-property) for details.
     * @returns {Request}
     */
    listTables: client.listTables.bind(client),
    /**
     * Get table information. Passthrough to [DynamoDB.describeTable](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#describTable-property).
     *
     * @instance
     * @memberof client
     * @param {object} params - request parameters. See [DynamoDB.describeTable](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#describeTable-property) for details.
     * @param {function} [callback] - a function to handle the response. See [DynamoDB.describeTable](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#describeTable-property) for details.
     * @returns {Request}
     */
    describeTable: client.describeTable.bind(client),
    /**
     * Perform a batch of get operations. Passthrough to [DocumentClient.batchGet](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#batchGet-property).
     *
     * @instance
     * @memberof client
     * @param {object} params - request parameters. See [DocumentClient.batchGet](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#batchGet-property) for details.
     * @param {function} [callback] - a function to handle the response. See [DocumentClient.batchGet](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#batchGet-property) for details.
     * @returns {Request}
     */
    batchGetItem: tableFreeDocClient.batchGet.bind(tableFreeDocClient),
    /**
     * Perform a batch of write operations. Passthrough to [DocumentClient.batchWrite](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#batchWrite-property).
     *
     * @instance
     * @memberof client
     * @param {object} params - request parameters. See [DocumentClient.batchWrite](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#batchWrite-property) for details.
     * @param {function} [callback] - a function to handle the response. See [DocumentClient.batchWrite](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#batchWrite-property) for details.
     * @returns {Request}
     */
    batchWriteItem: tableFreeDocClient.batchWrite.bind(tableFreeDocClient),
    /**
     * Delete a single record. Passthrough to [DocumentClient.delete](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#delete-property).
     *
     * @instance
     * @memberof client
     * @param {object} params - request parameters. See [DocumentClient.delete](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#delete-property) for details.
     * @param {function} [callback] - a function to handle the response. See [DocumentClient.delete](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#delete-property) for details.
     * @returns {Request}
     */
    deleteItem: docClient.delete.bind(docClient),
    /**
     * Get a single record. Passthrough to [DocumentClient.get](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#get-property).
     *
     * @instance
     * @memberof client
     * @param {object} params - request parameters. See [DocumentClient.get](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#get-property) for details.
     * @param {function} [callback] - a function to handle the response. See [DocumentClient.get](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#get-property) for details.
     * @returns {Request}
     */
    getItem: docClient.get.bind(docClient),
    /**
     * Put a single record. Passthrough to [DocumentClient.put](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#put-property).
     *
     * @instance
     * @memberof client
     * @param {object} params - request parameters. See [DocumentClient.put](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#put-property) for details.
     * @param {function} [callback] - a function to handle the response. See [DocumentClient.put](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#put-property) for details.
     * @returns {Request}
     */
    putItem: docClient.put.bind(docClient),
    /**
     * Update a single record. Passthrough to [DocumentClient.update](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#update-property).
     *
     * @instance
     * @memberof client
     * @param {object} params - request parameters. See [DocumentClient.update](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#update-property) for details.
     * @param {function} [callback] - a function to handle the response. See [DocumentClient.update](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#update-property) for details.
     * @returns {Request}
     */
    updateItem: docClient.update.bind(docClient)
  };

  /**
   * An array of [AWS.Requests](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Request.html)
   *
   * @name RequestSet
   */

  /**
   * Send all the requests in a set, optionally specifying concurrency. The
   * emphasis is on making it transparent to the caller the exact outcome of each
   * request in the set.
   *
   * The callback function will be passed arguments in this order:
   * - error: set to null if no errors occurred, otherwise an array of errors with
   * indexes that correspond to the indexes of the original request set
   * - responses: always an array of responses equal with indexes corresponding
   * to the original request set. If a particular request encountered an error,
   * that index in the `responses` array will be set to `null`.
   * - unprocessed: set to null if no unprocessed results were detected, otherwise
   * a new set of requests with its own .sendAll function bound to it. Again,
   * indexes correspond to those in the original request set.
   *
   * @name sendAll
   * @instance
   * @memberof RequestSet
   * @param {number} [concurrency] - the concurrency with which to make requests.
   * Default value is `1`.
   * @param {function} callback - a function to handle the response.
   */

  /**
   * An array of [AWS.Requests](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Request.html)
   *
   * @name CompleteRequestSet
   */

  /**
   * Send all the requests in a set, optionally specifying concurrency. This function
   * will retry unprocessed items and return a single response body aggregated from
   * results of all the individual requests.
   *
   * The callback function will be passed a single error object if any occurred, and the
   * aggregated response body. If all requests encountered an error, the second argument
   * will be null. Otherwise the callback may be provided with an error as well as
   * the outcome from successful requests.
   *
   * @name sendAll
   * @instance
   * @memberof CompleteRequestSet
   * @param {number} [concurrency] - the concurrency with which to make requests.
   * Default value is `1`.
   * @param {function} callback - a function to handle the response.
   */

  var dynoExtensions = {
    /**
     * Break a large batch of get operations into a set of requests that can be
     * sent individually or concurrently.
     *
     * @instance
     * @memberof client
     * @param {object} params - unbounded batchGetItem request parameters. See [DocumentClient.batchGet](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#batchGet-property) for details.
     * @returns {RequestSet}
     */
    batchGetItemRequests: require('./lib/requests')(docClient).batchGetItemRequests,
    /**
     * Break a large batch of write operations into a set of requests that can be
     * sent individually or concurrently.
     *
     * @instance
     * @memberof client
     * @param {object} params - unbounded batchWriteItem request parameters. See [DocumentClient.batchWrite](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#batchWrite-property) for details.
     * @returns {RequestSet}
     */
    batchWriteItemRequests: require('./lib/requests')(docClient).batchWriteItemRequests,
    /**
     * Break a large batch of get operations into a set of requests that are intended
     * to be sent concurrently.
     *
     * @instance
     * @memberof client
     * @param {object} params - unbounded batchGetItem request parameters. See [DocumentClient.batchGet](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#batchGet-property) for details.
     * @returns {CompleteRequestSet}
     */
    batchGetAll: require('./lib/requests')(docClient).batchGetAll,
    /**
     * Break a large batch of write operations into a set of requests that are intended
     * to be sent concurrently.
     *
     * @instance
     * @memberof client
     * @param {object} params - unbounded batchWriteItem request parameters. See [DocumentClient.batchWrite](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#batchWrite-property) for details.
     * @returns {CompleteRequestSet}
     */
    batchWriteAll: require('./lib/requests')(docClient).batchWriteAll,
    /**
     * Create a table. Passthrough to [DynamoDB.createTable](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#createTable-property),
     * except the function polls DynamoDB until the table is ready to accept
     * reads and writes, at which point the callback function is called.
     *
     * @instance
     * @memberof client
     * @param {object} params - request parameters. See [DynamoDB.createTable](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#createTable-property) for details.
     * @param {function} [callback] - a function to handle the response. See [DynamoDB.createTable](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#createTable-property) for details.
     * @returns {Request}
     */
    createTable: require('./lib/table')(client, null).create,
    /**
     * Delete a table. Passthrough to [DynamoDB.deleteTable](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#deleteTable-property),
     * except the function polls DynamoDB until the table is ready to accept
     * reads and writes, at which point the callback function is called.
     *
     * @instance
     * @memberof client
     * @param {object} params - request parameters. See [DynamoDB.deleteTable](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#deleteTable-property) for details.
     * @param {function} [callback] - a function to handle the response. See [DynamoDB.deleteTable](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#deleteTable-property) for details.
     * @returns {Request}
     */
    deleteTable: require('./lib/table')(client).delete,
    /**
     * Provide the results of a query as a [Readable Stream](https://nodejs.org/api/stream.html#stream_class_stream_readable).
     * This function will paginate through query responses, making HTTP requests
     * as the caller reads from the stream.
     *
     * @instance
     * @memberof client
     * @param {object} params - query request parameters. See [DocumentClient.query](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#query-property) for details.
     * @returns {ReadableStream}
     */
    queryStream: require('./lib/stream')(docClient).query,
    /**
     * Provide the results of a scan as a [Readable Stream](https://nodejs.org/api/stream.html#stream_class_stream_readable).
     * This function will paginate through query responses, making HTTP requests
     * as the caller reads from the stream.
     *
     * @instance
     * @memberof client
     * @param {object} params - scan request parameters. See [DocumentClient.scan](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#scan-property) for details.
     * @returns {ReadableStream}
     */
    scanStream: require('./lib/stream')(docClient).scan,
    /**
     * Creates a [Writable stream](https://nodejs.org/api/stream.html#stream_class_stream_writable).
     * Writing individual records to the stream will aggregate them into sets of
     * 25 items and submit them as `BatchWriteItem` requests.
     *
     * @param {object} options - stream options. See [Writable stream documentation](https://nodejs.org/api/stream.html#stream_class_stream_writable_1)
     * for available options. The stream **will always** set `objectMode: true` for you.
     * @param {number} [options.concurrency=1] - set the maximum desired concurrency for
     * outgoing BatchWriteItem requests.
     * @param {function} [options.retry] - sets the provided function as a [retry event listener](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Request.html#retry-event)
     * on outgoing BatchWriteItem requests.
     * @returns a [Writable stream](https://nodejs.org/api/stream.html#stream_class_stream_writable)
     */
    putStream: require('./lib/stream')(docClient, options.table).put,
    /**
     * Query a table or secondary index. Passthrough to [DocumentClient.query](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#query-property).
     *
     * @instance
     * @memberof client
     * @param {object} params - request parameters. See [DocumentClient.query](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#query-property) for details.
     * @param {number} [params.Pages=1] - maximum number of pages of query results to request. Set to `Infinity` to return all available data.
     * @param {function} [callback] - a function to handle the response. See [DocumentClient.query](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#query-property) for details.
     * @returns a Request if not paginating, or a ReadableStream if multiple pages were requested
     */
    query: require('./lib/paginated')(docClient).query,
    /**
     * Scan a table or secondary index. Passthrough to [DocumentClient.scan](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#scan-property).
     *
     * @instance
     * @memberof client
     * @param {object} params - request parameters. See [DocumentClient.scan](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#scan-property) for details.
     * @param {number} [params.Pages=1] - maximum number of pages of scan results to request. Set to `Infinity` to return all available data.
     * @param {function} [callback] - a function to handle the response. See [DocumentClient.scan](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#scan-property) for details.
     * @returns a Request if not paginating, or a ReadableStream if multiple pages were requested
     */
    scan: require('./lib/paginated')(docClient).scan
  };

  // Drop specific functions from read/write only clients
  if (options.read) {
    delete nativeFunctions.deleteItem;
    delete nativeFunctions.putItem;
    delete nativeFunctions.updateItem;
    delete nativeFunctions.batchWriteItem;
    delete dynoExtensions.batchWriteItemRequests;
    delete dynoExtensions.batchWriteAll;
    delete dynoExtensions.putStream;
  }

  if (options.write) {
    delete nativeFunctions.batchGetItem;
    delete nativeFunctions.getItem;
    delete dynoExtensions.query;
    delete dynoExtensions.scan;
    delete dynoExtensions.batchGetItemRequests;
    delete dynoExtensions.batchGetAll;
    delete dynoExtensions.queryStream;
    delete dynoExtensions.scanStream;
  }

  // Glue everything together
  return _({ config: config, defaultTable: options.table }).extend(nativeFunctions, dynoExtensions);
}

/**
 * Provides a dyno client capable of reading from one table and writing to another.
 *
 * @static
 * @param {object} readOptions - configuration parameters for the read table.
 * @param {object} writeOptions - configuration parameters for the write table.
 * @returns {dyno} a dyno client.
 */
Dyno.multi = function(readOptions, writeOptions) {
  var read = Dyno(_({}).extend(readOptions, { read: true }));
  var write = Dyno(_({}).extend(writeOptions, { write: true }));

  return _({}).extend(write, read, {
    config: { read: read.config, write: write.config },
    createTable: require('./lib/table')(read, write).multiCreate,
    deleteTable: require('./lib/table')(read, write).multiDelete
  });
};

/**
 * Create a DynamoDB set. When writing records to DynamoDB, arrays are interpretted
 * as `List` type attributes. Use this function to utilize a `Set` instead.
 *
 * @param {array} list - an array of strings, numbers, or buffers to store in
 * DynamoDB as a set
 * @returns {object} a DynamoDB set
 * @example
 * // This record will store the `data` attribute as a `List` in DynamoDB
 * var asList = {
 *  id: 'my-record',
 *  data: [1, 2, 3]
 * };
 *
 * // This record will store the `data` attribute as a `NumberSet` in DynamoDB
 * var asNumberSet = {
 *  id: 'my-record',
 *  data: Dyno.createSet([1, 2, 3]);
 * };
 */
Dyno.createSet = function(list) {
  return new DynamoDBSet(list);
};

/**
 * Convert a JavaScript object into a wire-formatted string that can be sent to
 * DynamoDB in an HTTP request.
 *
 * @param {object} item - a JavaScript object representing a DynamoDB record
 * @returns {string} the serialized representation of the record
 * @example
 * var item = {
 *  id: 'my-record',
 *  version: 2,
 *  data: new Buffer.from('Hello World!')
 * };
 * console.log(Dyno.serialize(item));
 * // {"id":{"S":"my-record"},"version":{"N":"2"},"data":{"B":"SGVsbG8gV29ybGQh"}}
 */
Dyno.serialize = require('./lib/serialization').serialize;
/**
 * Convert a wire-formatted string into a JavaScript object
 *
 * @param {string} str - the serialized representation of a DynamoDB record
 * @returns {object} a JavaScript object representing the record
 * @example
 * var str = '{"id":{"S":"my-record"},"version":{"N":"2"},"data":{"B":"SGVsbG8gV29ybGQh"}}';
 * console.log(Dyno.deserialize(str));
 * // {
 * //   id: 'my-record',
 * //   version: 2,
 * //   data: <Buffer 48 65 6c 6c 6f 20 57 6f 72 6c 64 21>
 * // }
 */
Dyno.deserialize = require('./lib/serialization').deserialize;
