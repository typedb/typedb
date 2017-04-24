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


import Visualiser from '../src/js/visualiser/Visualiser';
import * as GlobalMocks from './modules/GlobalMocks';

beforeAll(() => {
  GlobalMocks.MockLocalStorage();
});


test('Visualiser add 1 node and 1 malformed node: check first node exists in graph and second does not.', () => {
  const v = new Visualiser();
  v.addNode({ id: 'id-1' }, {});
  expect(v.nodeExists('id-1')).toBeTruthy();
  v.addNode('errorerror', { id: 'id-2' }, {});
  expect(v.nodeExists('id-2')).toBeFalsy();
});

test('Visualiser try to add edge from non existing node to and existing one : check edge is not created', () => {
  const v = new Visualiser();

  v.addNode({ id: 'id-2' }, {})
   .addEdge('id-1', 'id-2', 'e');
  expect(v.alreadyConnected('id-1', 'id-2', 'e')).toBeFalsy();
});

test('Visualiser add edge between two existing node: check the edge is created', () => {
  const v = new Visualiser();

  v.addNode({ id: 'id-1' }, {})
    .addNode({ id: 'id-2' }, {})
   .addEdge('id-1', 'id-2', 'e');

  expect(v.alreadyConnected('id-1', 'id-2', 'e')).toBeTruthy();
});

test('Visualiser test \'alreadyConnected\' method', () => {
  const v = new Visualiser();

  v.addNode({ id: 'id-1' }, {})
    .addNode({ id: 'id-2' }, {})
    .addNode({ id: 'id-3' }, {})
   .addEdge('id-1', 'id-2', 'e');

  expect(v.alreadyConnected('id-1', 'id-2', 'e')).toBeTruthy();
  expect(v.alreadyConnected('id-1', 'id-3', 'e')).toBeFalsy();
});
