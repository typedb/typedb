import test from 'ava';
import _ from 'underscore';

import Visualiser from '../src/js/visualiser/Visualiser';

test('Visualiser add node', t => {
    var v = new Visualiser();

    v.addNode(1);
    t.true(v.nodeExists(1));
});

test("Visualiser add edge", t => {
    var v = new Visualiser();
    v.addNode(1).addNode(2);
    v.addEdge(1, 2, 'e');

    t.true(v.alreadyConnected(1, 2));
});

test("Visualiser alreadyConnected", t => {
    var v = new Visualiser();
    v.addNode(1).addNode(2).addNode(3).addEdge(1,3);

    t.plan(2);
    t.true(v.alreadyConnected(1,3));
    t.false(v.alreadyConnected(1,2));
});

test("Visualiser multiple edges alreadyConnected", t => {
    var v = new Visualiser();
    v.addNode(1).addNode(2).addNode(3)
        .addEdge(1,2).addEdge(1,3);

    t.true(v.alreadyConnected(1,3));
});

test("Visualiser undefined alreadyConnected", t => {
    var v = new Visualiser();

    // No nodes or edges defined
    t.false(v.alreadyConnected(1,2));
});
