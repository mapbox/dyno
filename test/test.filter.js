var test = require('tape');
var BuildFilter = require('../lib/scan.js').BuildFilter;

function testbfquery(exp) {
    opts = {filter: exp};
    params = {};
    return BuildFilter(opts, params).FilterExpression;
}

test('scan.filter expression rewriting', function(t) {

    // simple operators
    t.equal(testbfquery('a>2'), 'a > :p0', '>');
    t.equal(testbfquery('xyz<3'), 'xyz < :p0', '<');
    t.equal(testbfquery('x<>10'), 'x <> :p0', '<>');
    t.equal(testbfquery('a >= 2'), 'a >= :p0', '>=');
    t.equal(testbfquery('a <=2.872'), 'a <= :p0', '<=');
    t.equal(testbfquery('q=9'), 'q = :p0', '=');

    // complex operators
    t.equal(testbfquery('between 1 and 2'), 'between :p0 and :p1', 'between');
    t.equal(testbfquery('x in (1, 2, 3,4)'), 'x in ( :p0, :p1, :p2, :p3 )', 'in 1');
    t.equal(testbfquery('x in (1,2,3) and y>0'), 'x in ( :p0, :p1, :p2 ) and y > :p3', 'in 2');
    t.equal(testbfquery('a >= 2 and b<3'), 'a >= :p0 and b < :p1', 'chained conditionals');
    t.equal(testbfquery('a >= 2 and (b<3)'), 'a >= :p0 and ( b < :p1 )', 'chained conditionals/parens');

    // quotes
    t.equal(testbfquery('s="testing"'), 's = :p0', 'quotes 1');
    t.equal(testbfquery('s=\'testing\''), 's = :p0', 'quotes 2');
    t.equal(testbfquery('s="testing" and R=\'testing2\''), 's = :p0 and R = :p1', 'quotes 3');
    t.equal(testbfquery('s="testing" and r>0'), 's = :p0 and r > :p1', 'quotes 4');

    // parens
    t.equal(testbfquery('((a=1 and b=2) or c=3)'), '( ( a = :p0 and b = :p1 ) or c = :p2 )', 'parens');

    // stress
    t.equal(testbfquery('a >= 2 and (c in (1) or d in ("a", 2)) and (b<3)'), 'a >= :p1 and ( c in ( :p2 ) or d in ( :p0, :p3 ) ) and ( b < :p4 )', 'stress');

    t.end();
});
