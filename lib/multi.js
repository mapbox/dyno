var _ = require('underscore');

module.exports = function(read, write) {
    return {
      getItem: function multiGetItem(key, opts, cb) {
          opts = _(opts).clone();
          if (opts.table) delete opts.table;
          read.getItem(key, opts, cb);
      },

      putItem: function multiPutItem(doc, opts, cb) {
          opts = _(opts).clone();
          if (opts.table) delete opts.table;
          write.putItem(doc, opts, cb);
      },

      updateItem: function multiUpdateItem(key, doc, opts, cb) {
          opts = _(opts).clone();
          if (opts.table) delete opts.table;
          write.updateItem(key, doc, opts, cb);
      },

      deleteItem: function multiDeleteItem(key, opts, cb) {
          opts = _(opts).clone();
          if (opts.table) delete opts.table;
          write.deleteItem(key, opts, cb);
      },

      query: function multiQuery(conditions, opts, cb) {
          opts = _(opts).clone();
          if (opts.table) delete opts.table;
          read.query(conditions, opts, cb);
      },

      scan: function multiScan(opts, cb) {
          opts = _(opts).clone();
          if (opts.table) delete opts.table;
          read.scan(conditions, opts, cb);
      },

      getItems: function multiGetItems(keys, opts, cb) {
          opts = _(opts).clone();
          if (opts.table) delete opts.table;
          read.getItems(keys, opts, cb);
      },

      putItems: function multiPutItems(items, opts, cb) {
          opts = _(opts).clone();
          if (opts.table) delete opts.table;
          write.putItems(keys, opts, cb);
      },

      deleteItems: function multiDeleteItems(itemKeys, opts, cb) {
          opts = _(opts).clone();
          if (opts.table) delete opts.table;
          write.deleteItems(keys, opts, cb);
      }
    };
};
