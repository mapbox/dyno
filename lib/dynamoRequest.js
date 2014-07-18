var stream = require('stream');

module.exports = function(opts, callback) {
    var dr = new stream.Readable({objectMode:true});
    dr.query = opts.query;
    var items = [];
    var read = false;

    opts.func(dr.query, function(err, resp){
        if(err && !callback) return dr.emit('error', err);
        if(err) return callback(err);

        var i =0;
        while(!callback && resp.items && i < resp.items.length) {
            items.push(resp.items[i]);
            i++;
        };

        while(read && items.length > 0) {
            dr.push(items.shift());
        }

        // if there is a next page, continue. If not end
        if(read) dr.push(null);

        if(callback) return callback(err, resp);
    });

    dr._read = function(size) {
        read = true;
        while(items.length > 0) {
            dr.push(items.shift());
        }
    };

    return dr;
}
