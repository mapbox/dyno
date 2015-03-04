module.exports = function(config) {
    var tables = {};

    tables.createTable = function(table, cb) {
        function check() {
            config.dynamo.describeTable({TableName: table.TableName}, function(err, data) {
                if (err && err.code === 'ResourceNotFoundException') {
                    config.dynamo.createTable(table, function(err) {
                        if (err) return cb(err);
                        setTimeout(check, 0);
                    });

                } else if (err) {
                    cb(err);

                } else if (data.Table.TableStatus === 'ACTIVE') {
                    cb(null, {});

                } else {
                    setTimeout(check, 1000);
                }
            });
        }

        check();
    };

    tables.deleteTable = function(table, cb) {
        if (typeof table === 'string')
            table = {TableName: table};

        function check() {
            config.dynamo.describeTable({TableName: table.TableName}, function(err, data) {
                if (err && err.code === 'ResourceNotFoundException') {
                    cb(null, {});

                } else if (err) {
                    cb(err);

                } else if (data.Table.TableStatus === 'ACTIVE') {
                    config.dynamo.deleteTable({TableName: table.TableName}, function(err) {
                        if (err) return cb(err);
                        setTimeout(check, 0);
                    });

                } else {
                    setTimeout(check, 1000);
                }
            });
        }

        check();
    };

    return tables;
};
