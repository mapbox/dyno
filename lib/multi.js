var _ = require('underscore');
var queue = require('queue-async');

module.exports = function(read, write) {
    return {
      createTable: function multiCreateTable(table, cb) {
          queue(1)
              .defer(read.createTable, table)
              .defer(write.createTable, table)
              .await(cb);
      },

      deleteTable: function multiDeleteTable(tableName, cb) {
          queue(1)
              .defer(read.deleteTable, tableName)
              .defer(write.deleteTable, tableName)
              .await(cb);
      },

      getItem: function multiGetItem(key, opts, cb) {
          if (typeof opts === 'function') {
              cb = opts;
              opts = {};
          }
          opts = opts ? _(opts).clone() : {};
          if (opts.table) delete opts.table;
          return read.getItem(key, opts, cb);
      },

      putItem: function multiPutItem(doc, opts, cb) {
          if (typeof opts === 'function') {
              cb = opts;
              opts = {};
          }
          opts = opts ? _(opts).clone() : {};
          if (opts.table) delete opts.table;
          return write.putItem(doc, opts, cb);
      },

      updateItem: function multiUpdateItem(key, doc, opts, cb) {
          if (typeof opts === 'function') {
              cb = opts;
              opts = {};
          }
          opts = opts ? _(opts).clone() : {};
          if (opts.table) delete opts.table;
          return write.updateItem(key, doc, opts, cb);
      },

      deleteItem: function multiDeleteItem(key, opts, cb) {
          if (typeof opts === 'function') {
              cb = opts;
              opts = {};
          }
          opts = opts ? _(opts).clone() : {};
          if (opts.table) delete opts.table;
          return write.deleteItem(key, opts, cb);
      },

      query: function multiQuery(conditions, opts, cb) {
          if (typeof opts === 'function') {
              cb = opts;
              opts = {};
          }
          opts = opts ? _(opts).clone() : {};
          if (opts.table) delete opts.table;
          return read.query(conditions, opts, cb);
      },

      scan: function multiScan(opts, cb) {
          if (typeof opts === 'function') {
              cb = opts;
              opts = {};
          }
          opts = opts ? _(opts).clone() : {};
          if (opts.table) delete opts.table;
          return read.scan(opts, cb);
      },

      getItems: function multiGetItems(keys, opts, cb) {
          if (typeof opts === 'function') {
              cb = opts;
              opts = {};
          }
          opts = opts ? _(opts).clone() : {};
          if (opts.table) delete opts.table;
          return read.getItems(keys, opts, cb);
      },

      putItems: function multiPutItems(items, opts, cb) {
          if (typeof opts === 'function') {
              cb = opts;
              opts = {};
          }
          opts = opts ? _(opts).clone() : {};
          if (opts.table) delete opts.table;
          return write.putItems(items, opts, cb);
      },

      deleteItems: function multiDeleteItems(itemKeys, opts, cb) {
          if (typeof opts === 'function') {
              cb = opts;
              opts = {};
          }
          opts = opts ? _(opts).clone() : {};
          if (opts.table) delete opts.table;
          return write.deleteItems(itemKeys, opts, cb);
      }
    };
};
