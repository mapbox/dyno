var types = require('./types');
var _ = require('underscore');
function BuildFilter(opts, params) {
    if (opts.filter.match(/(attribute_exists|attribute_not_exists|begins_with|contains)/i))
        throw new Error('FilterExpression functions are not currently supported.');
    params.FilterExpression = [];
    params.ExpressionAttributeValues = {};
    // count how many replacements we do
    var replacedTokens = 0;
    // replace string params
    var doubleq = /"(.*?)"/g;
    var singleq = /'(.*?)'/g;
    [doubleq, singleq].forEach(function(re) {
        while ((match = re.exec(opts.filter)) !== null) {
            opts.filter = opts.filter.replace(match[0], ':p' + replacedTokens);
            params.ExpressionAttributeValues[':p' + replacedTokens] = match[1].toString();
            replacedTokens++;
        }
    });
    // add whitespace to ensure split works properly
    var newFilter = '';
    for (var i = 0; i < opts.filter.length; i++) {
        switch (opts.filter[i]) {
            case '(':
                newFilter += ' ( ';
                break;
            case ')':
                newFilter += ' ) ';
                break;
            case ',':
                newFilter += ', ';
                break;
            default:
                newFilter += opts.filter[i];
                break;
        }
    }
    opts.filter = newFilter;
    // buffer operators
    ['<', '>', '='].forEach(function(operator) {
        opts.filter = opts.filter.replace(new RegExp(operator, 'g'), ' ' + operator + ' ');
    });
    // reassemble compound operators broken by the buffer
    opts.filter = opts.filter.replace(/<\s+>/g, '<>');
    opts.filter = opts.filter.replace(/>\s+=/g, '>=');
    opts.filter = opts.filter.replace(/<\s+=/g, '<=');
    var operators = ['between', 'in', 'and', 'or', '<', '>', '<=', '>=', '<>', '=', '(', ')'];
    var paramsToReplace = 0;
    opts.filter.split(/\s+/).forEach(function(token) {
        if (operators.indexOf(token.toLowerCase()) === -1) {
            if (paramsToReplace !== 0) {
                // skip tokens that have already been replaced
                if (token[0] === ':') {
                    params.FilterExpression.push(token);
                    if (paramsToReplace > 0)
                        paramsToReplace = paramsToReplace - 1;
                }
                else {
                    var hasComma = (token.lastIndexOf(',') === (token.length - 1));
                    if (hasComma) token = token.substring(0, token.length - 2);
                    var isNumeric = !token.match(/[^0-9\.\-]/i);
                    params.FilterExpression.push(':p' + replacedTokens + (hasComma ? ',' : ''));
                    params.ExpressionAttributeValues[':p' + replacedTokens] = (isNumeric ? parseFloat(token) : token);
                    replacedTokens++;
                    if (paramsToReplace > 0)
                        paramsToReplace = paramsToReplace - 1;
                }
            }
            else {
                params.FilterExpression.push(token);
            }
        }
        else {
            // reset param counter on closing paren if we're mid-"in"
            if ((token === ')') && (paramsToReplace < 0)) {
                paramsToReplace = 0;
            }
            else {
                if (token.match(/[<>=]/)) // simple operators
                    paramsToReplace = 1;
                else if (token.match(/(between)/i)) // between
                    paramsToReplace = 2;
                else if (token.match(/in/i)) { // in
                    paramsToReplace = -1;
                }
            }

            params.FilterExpression.push(token);
        }
    });

    params.FilterExpression = params.FilterExpression.join(' ').trim();
    params.ExpressionAttributeValues = types.toDynamoTypes(params.ExpressionAttributeValues);
    return params;
}

module.exports = function(config) {
    var dynamoRequest = require('./dynamoRequest')(config);
    return {
        /**
         * Scan a table
         * @param {Object} opts
         * @param {Function} cb
         */
        scan: function(opts, cb) {
            if (!cb && _.isFunction(opts)) {
                cb = opts;
                opts = {};
            }
            if (!opts) opts = {};
            var params = {
                TableName: opts.table || config.table
            };
            if (opts.hasOwnProperty('segment') && opts.segments) {
                params.Segment = opts.segment;
                params.TotalSegments = opts.segments;
            }

            if (opts.attributes) {
                params.AttributesToGet = opts.attributes;
                params.Select = 'SPECIFIC_ATTRIBUTES';
            } else {
                params.Select = opts.select || 'ALL_ATTRIBUTES';
            }

            if (opts.limit) {
                params.Limit = opts.limit;
            }

            if (opts.capacity) params.ReturnConsumedCapacity = opts.capacity;

            if (opts.filter) params = BuildFilter(opts, params);

            return dynamoRequest('scan', params, opts, cb);
        }
    };
};
module.exports.BuildFilter = BuildFilter;
