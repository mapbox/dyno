var util = require('util');
var _ = require('underscore');
var types = module.exports = {};

//  Convert to Dynamo's Object notation, unless it is already.
types.toDynamoTypes = function(obj) {
    if (Array.isArray(obj)) obj.map(query.toDynamoTypes);

    for (var key in obj) {
        var val = obj[key];
        if (typeof val === 'object' && !Array.isArray(val) && !Buffer.isBuffer(val)) {
            if (['N', 'S', 'B', 'NS', 'SS', 'BS'].indexOf(Object.keys(val)[0]) !== -1) {
                continue;
            } else {
                throw new Error('Unknown attribute ', val);
            }
        }

        if (typeof val === 'number') {
            obj[key] = { N: val.toString() };
        } else if (Array.isArray(val)) {
            var arr;
            var enc = 'utf8';
            if(typeof val[0] === 'number') {
                obj[key] = { NS: [] };
                arr = obj[key].NS;
                enc = 10;
            } else if(Buffer.isBuffer(val[0])) {
                obj[key] = { BS: [] };
                arr = obj[key].BS;
                enc = 'base64';
            } else {
                obj[key] = { SS: [] };
                arr = obj[key].SS;
            }

            val.forEach(stringifyAndPush);

            function stringifyAndPush(v) {
                arr.push(v.toString(enc));
            }
        } else if (Buffer.isBuffer(val)) {
            obj[key] = { B: val.toString('base64') };
        } else {
            obj[key] = { S: val.toString() };
        }
    }
    return obj;
}


// Convert types in the Dynamo request to normal objects.
types.typesFromDynamo = function(items) {
    if(!items) return;
    if(!util.isArray(items)) items = [items];

    return _(items).map(function(item) {
        _(item).each(function(v,k) {
            if(v.N) {
                item[k] =parseInt(v.N, 10);
            } else if(v.S) {
                item[k] =v.S;
            } else if(v.NS) {
                item[k] = _(v.NS).map(function(i){
                    return parseInt(i, 10);
                });
            } else if(v.SS) {
                item[k] = _(v.SS).map(function(i){
                    return i;
                });
            }
        });
        return item;
    });
};

function actionValue(attrs, action) {
    action = action.toUpperCase();
    attrs = types.toDynamoTypes(attrs);

    for(var a in attrs) {
        attrs[a] = {Action: action, Value: attrs[a]};
        if(action === 'DELETE') delete attrs[a].Value;
    }
    return attrs;
}

types.toAttributeUpdates = function(attrs, action) {
    var updateObj = {};

    for (var action in attrs) {
        _(updateObj).extend(actionValue(attrs[action], action));
   }
   return updateObj;
};
