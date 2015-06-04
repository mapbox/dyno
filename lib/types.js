var util = require('util');
var _ = require('underscore');
var types = module.exports = {};

var DynamoDBDatatype = require('dynamodb-doc/lib/datatypes').DynamoDBDatatype;
var datatypes = new DynamoDBDatatype();

types.createSet = datatypes.createSet;

//  Convert to Dynamo's Object notation, unless it is already.
types.toDynamoTypes = function(obj) {
    var attributeValueMap = {};
    if (Array.isArray(obj)) return obj.map(types.toDynamoTypes);
    if (typeof obj === 'string') return obj;
    for (var attr in obj) {
        var value = obj[attr];
        attributeValueMap[attr] = datatypes.formatDataType(value);
    }
    return attributeValueMap;
};

// Convert types in the Dynamo request to normal objects.
types.typesFromDynamo = function(items) {
    if (!items) return;
    if (!util.isArray(items)) items = [items];
    return formatItems(items);
};

function formatAttrValOutput(item) {
    var attrList = {};
    for (var attribute in item) {
        var keys = Object.keys(item[attribute]);
        var key = keys[0];
        var value = item[attribute][key];

        value = datatypes.formatWireType(key, value);
        attrList[attribute] = value;
    }

    return attrList;
}

function formatItems(items) {
    for (var index in items) {
        items[index] = formatAttrValOutput(items[index]);
    }
    return items;
}

function actionValue(attrs, action) {
    action = action.toUpperCase();
    attrs = types.toDynamoTypes(attrs);
    if (Array.isArray(attrs)) {
        var deletes = [];
        attrs.forEach(function(a) {
            deletes[a] = {Action: action};
        });
        attrs = deletes;
    } else {
        for (var a in attrs) {
            attrs[a] = {Action: action, Value: attrs[a]};
            if (action === 'DELETE' && attrs[a].Value.NULL) delete attrs[a].Value;
        }
    }
    return attrs;
}

types.toAttributeUpdates = function(attrs) {
    var updateObj = {};

    for (var action in attrs) {
        _(updateObj).extend(actionValue(attrs[action], action));
    }
    return updateObj;
};

types.conditions = function(conditions) {
    function pushAttributeValues(avlist) {
        this.push(types.toDynamoTypes({val: avlist}).val);
    }
    var keyConditions = {};
    for (var c in conditions) {
        var con = conditions[c];
        var compare = typeof con == 'string' ? con : Object.keys(con)[0];
        keyConditions[c] = {};
        keyConditions[c].ComparisonOperator = compare;
        keyConditions[c].AttributeValueList = [];
        if (!Array.isArray(con[compare])) con[compare] = [con[compare]];
        (con[compare] || []).forEach(pushAttributeValues.bind(keyConditions[c].AttributeValueList));
        if (keyConditions[c].AttributeValueList.length === 0)
            delete keyConditions[c].AttributeValueList;
    }
    return keyConditions;
};
