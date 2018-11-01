/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

import Visualiser from '../src/js/visualiser/Visualiser';
import * as GlobalMocks from './modules/GlobalMocks';


let v;

beforeAll(() => {
  GlobalMocks.MockLocalStorage();
  GlobalMocks.requestAnimationFramePolyFill();
  v = new Visualiser(10);
  const tempDiv = document.createElement('div');
  v.render(tempDiv);
});

afterEach(() => {
  v.clearGraph();
});


test('Visualiser add 1 node and 1 malformed node: check first node exists in graph and second does not.', () => {
  v.addNode({ id: 'id-1' }, {});
  expect(v.nodeExists('id-1')).toBeTruthy();
  v.addNode('errorerror', { id: 'id-2' }, {});
  expect(v.nodeExists('id-2')).toBeFalsy();
});

test('Visualiser try to add edge from non existing node to and existing one : check edge is not created', () => {
  v.addNode({ id: 'id-2' }, {})
   .addEdge('id-1', 'id-2', 'e');
  expect(v.alreadyConnected('id-1', 'id-2', 'e')).toBeFalsy();
});

test('Visualiser add edge between two existing node: check the edge is created', () => {
  v.addNode({ id: 'id-1' }, {})
    .addNode({ id: 'id-2' }, {})
   .addEdge('id-1', 'id-2', 'e');

  expect(v.alreadyConnected('id-1', 'id-2', 'e')).toBeTruthy();
});

test('Visualiser test \'alreadyConnected\' method', () => {
  v.addNode({ id: 'id-1' }, {})
    .addNode({ id: 'id-2' }, {})
    .addNode({ id: 'id-3' }, {})
   .addEdge('id-1', 'id-2', 'e');

  expect(v.alreadyConnected('id-1', 'id-2', 'e')).toBeTruthy();
  expect(v.alreadyConnected('id-1', 'id-3', 'e')).toBeFalsy();
});

test('Visualiser test when deleting a node also delete all its edges', () => {
  v.addNode({ id: 'id-1' }, {})
    .addEdge('id-1', 'id-2', 'a')
    .addEdge('id-1', 'id-2', 'b')
    .addEdge('id-1', 'id-3', 'c');

  v.deleteNode('id-1');

  expect(v.getNode('id-1')).toBeNull();
  expect(v.edges.get().length).toBe(0);
});

test('Visualiser test multiple attempts to insert the same edge will result in only one insertion', () => {
  v.addNode({ id: 'id-1' }, {})
    .addNode({ id: 'id-2' }, {})
   .addEdge('id-1', 'id-2', 'e')
   .addEdge('id-1', 'id-2', 'e')
   .addEdge('id-1', 'id-2', 'e');

  expect(v.edges.get().length).toBe(1);
});

test('Visualiser test insertion of multiple edges with different label between same two nodes will insert them all', () => {
  v.addNode({ id: 'id-1' }, {})
    .addNode({ id: 'id-2' }, {})
   .addEdge('id-1', 'id-2', 'e')
   .addEdge('id-1', 'id-2', 'f')
   .addEdge('id-1', 'id-2', 'g');

  expect(v.edges.get().length).toBe(3);
});


test('Visualiser test 2 edges from and to the same nodes have \'smooth\' param set to object', () => {
  v.addNode({ id: 'id-1' }, {})
    .addNode({ id: 'id-2' }, {})
   .addEdge('id-1', 'id-2', 'e')
   .addEdge('id-1', 'id-2', 'b');

  v.edges.get().forEach((edge) => {
      expect(edge.smooth instanceof Object).toBeTruthy();
   });
});

test('Visualiser test 1 edge from and to a node does not have \'smooth\' param', () => {
  v.addNode({ id: 'id-1' }, {})
    .addNode({ id: 'id-2' }, {})
   .addEdge('id-1', 'id-2', 'e');

  expect(('smooth' in v.nodes.get()[0])).toBeFalsy();
});
