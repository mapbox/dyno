## `Dyno`

Dyno constructor

### Parameters

* `config` **`Object`** a configuration object





## `createTable`

Create a table

### Parameters

* `table` **`String`** 
* `cb` **`Function`** 





## `deleteItem`

Delete an item from a table

### Parameters

* `key` **`String`** 
* `opts` **`Object`** 
* `cb` **`Function`** callback





## `deleteTable`

Delete a table

### Parameters

* `table` **`String`** 
* `cb` **`Function`** 





## `getItem`

Get an item from a table

### Parameters

* `key` **`String`** key of the item to get
* `opts` **`Object`** 
* `cb` **`Function`** callback





## `putItem`

Put an item into a table

### Parameters

* `doc` **`Object`** document to put to the DynamoDB table
* `opts` **`[Object]`** options that modify item put and are passed to the DynamoDB request
* `callback` **`Function`** 


### Examples

```js
dyno.putItem({
    id: 'yo',
    range: 5,
    subject: 'test'
}, {
     expected: {
         likes: { 'LE': 1000 }
     }
}, function(err, resp) {
    // called asynchronously
});
```



## `scan`

Scan a table

### Parameters

* `opts` **`Object`** 
* `cb` **`Function`** 





## `updateItem`

Update an item in a table

### Parameters

* `key` **`String`** the primary key of the item to be updated
* `updates` **`Object`** the names of attributes to be modified, the action to perform on each, and the new value for each
* `opts` **`[Object]`** options that modify item update and are passed to the DynamoDB request
* `callback` **`Function`** 


### Examples

```js
dyno.updateItem({
    id: 'yo',
    range: 5
}, {
    put: {
        subject: 'oh hai',
        message: 'kthxbai'
    },
    add: { likes: 1 }
}, {
     expected: {
         likes: { 'LE': 1000 }
     }
}, function(err, resp) {
    // called asynchronously
});
```



