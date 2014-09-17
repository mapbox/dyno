var util = require('util');
var _ = require('underscore');
var types = module.exports = {};

//  Convert to Dynamo's Object notation, unless it is already.
types.toDynamoTypes = function(obj) {
    var obj = _(obj).clone();
    if (Array.isArray(obj)) return obj.map(types.toDynamoTypes);

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
            if(typeof val[0] === 'number') {
                obj[key] = { NS: [] };
                arr = obj[key].NS;
                val.forEach(function(v){ arr.push(v.toString());});
            } else if(Buffer.isBuffer(val[0])) {
                obj[key] = { BS: [] };
                arr = obj[key].BS;
                val.forEach(function(v){ arr.push(v);});
            } else {
                obj[key] = { SS: [] };
                arr = obj[key].SS;
                val.forEach(function(v){ arr.push(v.toString('utf8'));});
            }
        } else if (Buffer.isBuffer(val)) {
            obj[key] = { B: val };
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
                item[k] = parseFloat(v.N);
            } else if(v.S) {
                item[k] =v.S;
            } else if(v.NS) {
                item[k] = _(v.NS).map(function(i){
                    return parseFloat(i);
                });
            } else if(v.SS) {
                item[k] = _(v.SS).map(function(i){
                    return i;
                });
            } else if(v.B) {
                item[k] = v.B;
            } else if(v.BS) {
                item[k] = _(v.BS).map(function(i){
                    return new Buffer(i);
                });
            }
        });
        return item;
    });
};

function actionValue(attrs, action) {
    action = action.toUpperCase();
    attrs = types.toDynamoTypes(attrs);
    if(action === 'DELETE') {
        var deletes = {};
        attrs.forEach(function(a){
            deletes[a] = {Action: action};
        });
        attrs = deletes;
    } else {
        for(var a in attrs) {
            attrs[a] = {Action: action, Value: attrs[a]};
        }
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

types.conditions = function(conditions) {
    var keyConditions = {};
    for(c in conditions) {
        var con = conditions[c];
        var compare = Object.keys(con)[0];
        keyConditions[c] = {};
        keyConditions[c].ComparisonOperator = compare;
        keyConditions[c].AttributeValueList = [];
        if(!Array.isArray(con[compare])) con[compare] = [con[compare]];
        con[compare].forEach(function(avlist){
            keyConditions[c].AttributeValueList.push(types.toDynamoTypes({val:avlist}).val);
        });
    }
    return keyConditions;
}
