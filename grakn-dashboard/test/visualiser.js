/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

import test from 'ava';
import _ from 'underscore';

import Visualiser from '../src/js/visualiser/Visualiser';

var defaultBP = {
    label: "test node",
    type: "test",
    baseType: "test",
    ontology: "/ontology/test"
};

// Tests need to be re-written and updated
//
// test('Visualiser add node', t => {
//     var v = new Visualiser();
//
//     v.addNode(1, defaultBP, {});
//     t.true(v.nodeExists(1));
// });
//
// test("Visualiser add edge", t => {
//     var v = new Visualiser();
//     v.addNode(1, defaultBP, {}).addNode(2, defaultBP, {});
//     v.addEdge(1, 2, 'e');
//
//     t.true(v.alreadyConnected(1, 2));
// });
//
// test("Visualiser alreadyConnected", t => {
//     var v = new Visualiser();
//     v.addNode(1, defaultBP, {})
//      .addNode(2, defaultBP, {})
//      .addNode(3, defaultBP, {})
//      .addEdge(1,3);
//
//     t.plan(2);
//     t.true(v.alreadyConnected(1,3));
//     t.false(v.alreadyConnected(1,2));
// });
//
// test("Visualiser multiple edges alreadyConnected", t => {
//     var v = new Visualiser();
//     v.addNode(1, defaultBP, {})
//      .addNode(2, defaultBP, {})
//      .addNode(3, defaultBP, {})
//      .addEdge(1,2)
//      .addEdge(1,3);
//
//     t.true(v.alreadyConnected(1,3));
// });

test("Visualiser undefined alreadyConnected", t => {
    var v = new Visualiser();

    // No nodes or edges defined
    t.false(v.alreadyConnected(1,2));
});
