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

import Parser from '../src/js/Parser/Parser';
import * as MockedResponses from './modules/MockedEngineResponses';
import * as GlobalMocks from './modules/GlobalMocks';
import _ from 'underscore';


Array.prototype.flatMap = function (lambda) {
  return Array.prototype.concat.apply([], this.map(lambda));
};

beforeAll(() => {
  GlobalMocks.MockLocalStorage();
});

test('Parse person instance', () => {
  const personInstanceResponse = {
      type: {
        label: 'person',
        '@id': '/kb/gene/type/person',
      },
      attributes: [],
      keys: [],
      relationships: [
        {
          role: '/kb/gene/role/spouse2',
          thing: '/kb/gene/concept/V176208',
        },
        {
          role: '/kb/gene/role/child',
          thing: '/kb/gene/concept/V159776',
        },
        {
          role: '/kb/gene/role/child',
          thing: '/kb/gene/concept/V168016',
        },
      ],
      'base-type': 'ENTITY',
      id: 'V41080',
      '@id': '/kb/gene/concept/V41080',
    };
  
  const parsed = Parser.parseResponse(personInstanceResponse);
  expect(parsed.nodes[0].id).toBe('V41080');
  expect(parsed.nodes[0].baseType).toBe('ENTITY');
  expect(parsed.nodes[0].href).toBe('/kb/gene/concept/V41080');
  expect(parsed.nodes[0].type).toBe('person');
  expect(parsed.nodes[0].relationships.length).toBe(3);
});

test('Parse array of person instances', () => {
  const personsArray = [
    {
      x: {
        type: {
          label: 'person',
          '@id': '/kb/gene/type/person',
        },
        attributes: [],
        keys: [],
        relationships: [
          {
            role: '/kb/gene/role/spouse2',
            thing: '/kb/gene/concept/V176208',
          },
          {
            role: '/kb/gene/role/child',
            thing: '/kb/gene/concept/V159776',
          },
          {
            role: '/kb/gene/role/child',
            thing: '/kb/gene/concept/V168016',
          },
        ],
        'base-type': 'ENTITY',
        id: 'V41080',
        '@id': '/kb/gene/concept/V41080',
      },
    },
    {
      x: {
        type: {
          label: 'person',
          '@id': '/kb/gene/type/person',
        },
        attributes: [],
        keys: [],
        relationships: [
          {
            role: '/kb/gene/role/parent',
            thing: '/kb/gene/concept/V172136',
          },
          {
            role: '/kb/gene/role/parent',
            thing: '/kb/gene/concept/V147560',
          },
          {
            role: '/kb/gene/role/spouse1',
            thing: '/kb/gene/concept/V184464',
          },
          {
            role: '/kb/gene/role/parent',
            thing: '/kb/gene/concept/V127096',
          },
        ],
        'base-type': 'ENTITY',
        id: 'V41104',
        '@id': '/kb/gene/concept/V41104',
      },
    },
  ];
  const parsed = Parser.parseResponse(personsArray);
  expect(parsed.nodes.length).toBe(2);
  expect(parsed.nodes[0].id).toBe('V41080');
  expect(parsed.nodes[0].baseType).toBe('ENTITY');
  expect(parsed.nodes[0].href).toBe('/kb/gene/concept/V41080');
  expect(parsed.nodes[0].type).toBe('person');
  expect(parsed.nodes[0].relationships.length).toBe(3);
  expect(parsed.nodes[1].id).toBe('V41104');
  expect(parsed.nodes[1].baseType).toBe('ENTITY');
  expect(parsed.nodes[1].href).toBe('/kb/gene/concept/V41104');
  expect(parsed.nodes[1].type).toBe('person');
  expect(parsed.nodes[1].relationships.length).toBe(4);
});

test('Parse edges from Schema response', () => {
  const schemaResponse = [
    {
      x: {
        label: 'marriage',
        implicit: false,
        subs: [
          '/kb/gene/type/marriage',
        ],
        plays: [
          '/kb/gene/role/@has-picture-owner',
        ],
        attributes: [
          '/kb/gene/type/picture',
        ],
        keys: [],
        relates: [
          '/kb/gene/role/wife',
          '/kb/gene/role/spouse2',
          '/kb/gene/role/spouse1',
          '/kb/gene/role/husband',
        ],
        'base-type': 'RELATIONSHIP_TYPE',
        id: 'V57416',
        '@id': '/kb/gene/type/marriage',
        super: '/kb/gene/type/relationship',
        abstract: false,
      },
    },
    {
      x: {
        label: 'person',
        implicit: false,
        subs: [
          '/kb/gene/type/person',
        ],
        plays: [
          '/kb/gene/role/daughter',
          '/kb/gene/role/@has-gender-owner',
          '/kb/gene/role/mother-in-law',
          '/kb/gene/role/@has-surname-owner',
          '/kb/gene/role/grandson',
          '/kb/gene/role/@has-age-owner',
          '/kb/gene/role/grandchild',
          '/kb/gene/role/@has-birth-date-owner',
          '/kb/gene/role/granddaughter',
          '/kb/gene/role/grandfather',
          '/kb/gene/role/husband',
          '/kb/gene/role/father',
          '/kb/gene/role/son',
          '/kb/gene/role/@has-death-date-owner',
          '/kb/gene/role/grandparent',
          '/kb/gene/role/grandmother',
          '/kb/gene/role/cousin',
          '/kb/gene/role/father-in-law',
          '/kb/gene/role/@has-firstname-owner',
          '/kb/gene/role/child-in-law',
          '/kb/gene/role/mother',
          '/kb/gene/role/sibling',
          '/kb/gene/role/@has-identifier-owner',
          '/kb/gene/role/@has-middlename-owner',
          '/kb/gene/role/wife',
          '/kb/gene/role/spouse2',
          '/kb/gene/role/daughter-in-law',
          '/kb/gene/role/spouse1',
          '/kb/gene/role/parent-in-law',
          '/kb/gene/role/parent',
          '/kb/gene/role/son-in-law',
          '/kb/gene/role/child',
          '/kb/gene/role/@has-picture-owner',
        ],
        attributes: [
          '/kb/gene/type/picture',
          '/kb/gene/type/gender',
          '/kb/gene/type/middlename',
          '/kb/gene/type/birth-date',
          '/kb/gene/type/identifier',
          '/kb/gene/type/firstname',
          '/kb/gene/type/age',
          '/kb/gene/type/surname',
          '/kb/gene/type/death-date',
        ],
        keys: [],
        'base-type': 'ENTITY_TYPE',
        id: 'V12320',
        '@id': '/kb/gene/type/person',
        super: '/kb/gene/type/entity',
        abstract: false,
      },
    }];
  const parsed = Parser.parseResponse(schemaResponse);
  expect(parsed.nodes.length).toBe(2);
});
