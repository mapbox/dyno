var stream = require('stream');

module.exports = function(opts, callback) {
    var dr = new stream.Readable({objectMode:true});
    dr.query = opts.query;
    var items = [];
    var read = false;
    var page = 0;

    if(opts.pages === undefined && callback) opts.pages = 1;
    if(!opts.pages) opts.pages = Infinity;

    function doRequest(start){
        if(start) dr.query.ExclusiveStartKey = start;
        opts.func(dr.query, function(err, resp){
            if(err && !callback) return dr.emit('error', err);
            if(err) return callback(err);

            page++;
            var i =0;
            while(resp.items && i < resp.items.length) {
                items.push(resp.items[i]);
                i++;
            };

            while(read && items.length > 0) {
                dr.push(items.shift());
            }

            // if there is a next page, continue. If not end
            if(((read && page < opts.pages) || (page < opts.pages)) && resp.last) {
                return doRequest(resp.last);
            } else if(read) {
                dr.push(null);
            }
            if(callback) {
                resp.items = items;
                return callback(err, resp);
            }
        });
    }
    doRequest();
    dr._read = function(size) {
        read = true;
        while(items.length > 0) {
            dr.push(items.shift());
        }
    };

    return dr;
}
