var query = require('./query');

module.exports = function(item, opts, callback) {
    var dr = {};

    var perparedItem = query.toDynamoTypes(item);

    dr.query = {};
    opts.func(dr.query)

    return dr;
}
