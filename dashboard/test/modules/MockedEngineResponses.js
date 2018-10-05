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

export const OntologyResponse = [{ x: { _baseType: 'TYPE', _name: 'thing', _links: { explore: [{ href: '/dashboard/explore/V4312?keyspace=modern&offsetEmbedded=0' }], self: { href: '/kb/concept/V4312?keyspace=modern&offsetEmbedded=0' } }, _id: 'V4312', _implicit: false } }, { x: { _baseType: 'ENTITY_TYPE', _name: 'entity', _links: { explore: [{ href: '/dashboard/explore/V4168?keyspace=modern&offsetEmbedded=0' }], self: { href: '/kb/concept/V4168?keyspace=modern&offsetEmbedded=0' } }, _embedded: { sub: [{ _direction: 'OUT', _baseType: 'TYPE', _name: 'thing', _links: { explore: [{ href: '/dashboard/explore/V4312?keyspace=modern&offsetEmbedded=0' }], self: { href: '/kb/concept/V4312?keyspace=modern&offsetEmbedded=0' } }, _id: 'V4312', _implicit: false }] }, _id: 'V4168', _implicit: false } }, { x: { _baseType: 'ATTRIBUTE_TYPE', _name: 'attribute', _links: { explore: [{ href: '/dashboard/explore/V8408?keyspace=modern&offsetEmbedded=0' }], self: { href: '/kb/concept/V8408?keyspace=modern&offsetEmbedded=0' } }, _embedded: { sub: [{ _direction: 'OUT', _baseType: 'TYPE', _name: 'thing', _links: { explore: [{ href: '/dashboard/explore/V4312?keyspace=modern&offsetEmbedded=0' }], self: { href: '/kb/concept/V4312?keyspace=modern&offsetEmbedded=0' } }, _id: 'V4312', _implicit: false }] }, _dataType: '', _id: 'V8408', _implicit: false } }, { x: { _baseType: 'RELATIONSHIP_TYPE', _name: 'relationship', _links: { explore: [{ href: '/dashboard/explore/V8264?keyspace=modern&offsetEmbedded=0' }], self: { href: '/kb/concept/V8264?keyspace=modern&offsetEmbedded=0' } }, _embedded: { sub: [{ _direction: 'OUT', _baseType: 'TYPE', _name: 'thing', _links: { explore: [{ href: '/dashboard/explore/V4312?keyspace=modern&offsetEmbedded=0' }], self: { href: '/kb/concept/V4312?keyspace=modern&offsetEmbedded=0' } }, _id: 'V4312', _implicit: false }] }, _id: 'V8264', _implicit: false } }, { x: { _baseType: 'ENTITY_TYPE', _name: 'person', _links: { explore: [{ href: '/dashboard/explore/V12504?keyspace=modern&offsetEmbedded=0' }], self: { href: '/kb/concept/V12504?keyspace=modern&offsetEmbedded=0' } }, _embedded: { sub: [{ _direction: 'OUT', _baseType: 'ENTITY_TYPE', _name: 'entity', _links: { explore: [{ href: '/dashboard/explore/V4168?keyspace=modern&offsetEmbedded=0' }], self: { href: '/kb/concept/V4168?keyspace=modern&offsetEmbedded=0' } }, _id: 'V4168', _implicit: false }] }, _id: 'V12504', _implicit: false } }, { x: { _baseType: 'ATTRIBUTE_TYPE', _name: 'age', _links: { explore: [{ href: '/dashboard/explore/V12360?keyspace=modern&offsetEmbedded=0' }], self: { href: '/kb/concept/V12360?keyspace=modern&offsetEmbedded=0' } }, _embedded: { sub: [{ _direction: 'OUT', _baseType: 'ATTRIBUTE_TYPE', _name: 'attribute', _links: { explore: [{ href: '/dashboard/explore/V8408?keyspace=modern&offsetEmbedded=0' }], self: { href: '/kb/concept/V8408?keyspace=modern&offsetEmbedded=0' } }, _dataType: '', _id: 'V8408', _implicit: false }] }, _dataType: 'java.lang.Long', _id: 'V12360', _implicit: false } }, { x: { _baseType: 'RELATIONSHIP_TYPE', _name: 'has-age', _links: { explore: [{ href: '/dashboard/explore/V4200?keyspace=modern&offsetEmbedded=0' }], self: { href: '/kb/concept/V4200?keyspace=modern&offsetEmbedded=0' } }, _embedded: { sub: [{ _direction: 'OUT', _baseType: 'RELATIONSHIP_TYPE', _name: 'relationship', _links: { explore: [{ href: '/dashboard/explore/V8264?keyspace=modern&offsetEmbedded=0' }], self: { href: '/kb/concept/V8264?keyspace=modern&offsetEmbedded=0' } }, _id: 'V8264', _implicit: false }] }, _id: 'V4200', _implicit: true } }, { x: { _baseType: 'ATTRIBUTE_TYPE', _name: 'name', _links: { explore: [{ href: '/dashboard/explore/V4232?keyspace=modern&offsetEmbedded=0' }], self: { href: '/kb/concept/V4232?keyspace=modern&offsetEmbedded=0' } }, _embedded: { sub: [{ _direction: 'OUT', _baseType: 'ATTRIBUTE_TYPE', _name: 'attribute', _links: { explore: [{ href: '/dashboard/explore/V8408?keyspace=modern&offsetEmbedded=0' }], self: { href: '/kb/concept/V8408?keyspace=modern&offsetEmbedded=0' } }, _dataType: '', _id: 'V8408', _implicit: false }] }, _dataType: 'java.lang.String', _id: 'V4232', _implicit: false } }, { x: { _baseType: 'RELATIONSHIP_TYPE', _name: 'has-name', _links: { explore: [{ href: '/dashboard/explore/V4096?keyspace=modern&offsetEmbedded=0' }], self: { href: '/kb/concept/V4096?keyspace=modern&offsetEmbedded=0' } }, _embedded: { sub: [{ _direction: 'OUT', _baseType: 'RELATIONSHIP_TYPE', _name: 'relationship', _links: { explore: [{ href: '/dashboard/explore/V8264?keyspace=modern&offsetEmbedded=0' }], self: { href: '/kb/concept/V8264?keyspace=modern&offsetEmbedded=0' } }, _id: 'V8264', _implicit: false }] }, _id: 'V4096', _implicit: true } }, { x: { _baseType: 'ENTITY_TYPE', _name: 'software', _links: { explore: [{ href: '/dashboard/explore/V8328?keyspace=modern&offsetEmbedded=0' }], self: { href: '/kb/concept/V8328?keyspace=modern&offsetEmbedded=0' } }, _embedded: { sub: [{ _direction: 'OUT', _baseType: 'ENTITY_TYPE', _name: 'entity', _links: { explore: [{ href: '/dashboard/explore/V4168?keyspace=modern&offsetEmbedded=0' }], self: { href: '/kb/concept/V4168?keyspace=modern&offsetEmbedded=0' } }, _id: 'V4168', _implicit: false }] }, _id: 'V8328', _implicit: false } }, { x: { _baseType: 'ATTRIBUTE_TYPE', _name: 'lang', _links: { explore: [{ href: '/dashboard/explore/V20552?keyspace=modern&offsetEmbedded=0' }], self: { href: '/kb/concept/V20552?keyspace=modern&offsetEmbedded=0' } }, _embedded: { sub: [{ _direction: 'OUT', _baseType: 'ATTRIBUTE_TYPE', _name: 'attribute', _links: { explore: [{ href: '/dashboard/explore/V8408?keyspace=modern&offsetEmbedded=0' }], self: { href: '/kb/concept/V8408?keyspace=modern&offsetEmbedded=0' } }, _dataType: '', _id: 'V8408', _implicit: false }] }, _dataType: 'java.lang.String', _id: 'V20552', _implicit: false } }, { x: { _baseType: 'RELATIONSHIP_TYPE', _name: 'has-lang', _links: { explore: [{ href: '/dashboard/explore/V8296?keyspace=modern&offsetEmbedded=0' }], self: { href: '/kb/concept/V8296?keyspace=modern&offsetEmbedded=0' } }, _embedded: { sub: [{ _direction: 'OUT', _baseType: 'RELATIONSHIP_TYPE', _name: 'relationship', _links: { explore: [{ href: '/dashboard/explore/V8264?keyspace=modern&offsetEmbedded=0' }], self: { href: '/kb/concept/V8264?keyspace=modern&offsetEmbedded=0' } }, _id: 'V8264', _implicit: false }] }, _id: 'V8296', _implicit: true } }, { x: { _baseType: 'RELATIONSHIP_TYPE', _name: 'knows', _links: { explore: [{ href: '/dashboard/explore/V28744?keyspace=modern&offsetEmbedded=0' }], self: { href: '/kb/concept/V28744?keyspace=modern&offsetEmbedded=0' } }, _embedded: { sub: [{ _direction: 'OUT', _baseType: 'RELATIONSHIP_TYPE', _name: 'relationship', _links: { explore: [{ href: '/dashboard/explore/V8264?keyspace=modern&offsetEmbedded=0' }], self: { href: '/kb/concept/V8264?keyspace=modern&offsetEmbedded=0' } }, _id: 'V8264', _implicit: false }] }, _id: 'V28744', _implicit: false } }, { x: { _baseType: 'ATTRIBUTE_TYPE', _name: 'weight', _links: { explore: [{ href: '/dashboard/explore/V8440?keyspace=modern&offsetEmbedded=0' }], self: { href: '/kb/concept/V8440?keyspace=modern&offsetEmbedded=0' } }, _embedded: { sub: [{ _direction: 'OUT', _baseType: 'ATTRIBUTE_TYPE', _name: 'attribute', _links: { explore: [{ href: '/dashboard/explore/V8408?keyspace=modern&offsetEmbedded=0' }], self: { href: '/kb/concept/V8408?keyspace=modern&offsetEmbedded=0' } }, _dataType: '', _id: 'V8408', _implicit: false }] }, _dataType: 'java.lang.Double', _id: 'V8440', _implicit: false } }, { x: { _baseType: 'RELATIONSHIP_TYPE', _name: 'has-weight', _links: { explore: [{ href: '/dashboard/explore/V28888?keyspace=modern&offsetEmbedded=0' }], self: { href: '/kb/concept/V28888?keyspace=modern&offsetEmbedded=0' } }, _embedded: { sub: [{ _direction: 'OUT', _baseType: 'RELATIONSHIP_TYPE', _name: 'relationship', _links: { explore: [{ href: '/dashboard/explore/V8264?keyspace=modern&offsetEmbedded=0' }], self: { href: '/kb/concept/V8264?keyspace=modern&offsetEmbedded=0' } }, _id: 'V8264', _implicit: false }] }, _id: 'V28888', _implicit: true } }, { x: { _baseType: 'RELATIONSHIP_TYPE', _name: 'programming', _links: { explore: [{ href: '/dashboard/explore/V12288?keyspace=modern&offsetEmbedded=0' }], self: { href: '/kb/concept/V12288?keyspace=modern&offsetEmbedded=0' } }, _embedded: { sub: [{ _direction: 'OUT', _baseType: 'RELATIONSHIP_TYPE', _name: 'relationship', _links: { explore: [{ href: '/dashboard/explore/V8264?keyspace=modern&offsetEmbedded=0' }], self: { href: '/kb/concept/V8264?keyspace=modern&offsetEmbedded=0' } }, _id: 'V8264', _implicit: false }] }, _id: 'V12288', _implicit: false } }];
export const PokemonInstance = [{ x: { _baseType: 'ENTITY', _links: { explore: [{ href: '/dashboard/explore/V16488?keyspace=pokemon&offsetEmbedded=0&limitEmbedded=5' }], self: { href: '/kb/concept/V16488?keyspace=pokemon&offsetEmbedded=5&limitEmbedded=5' } }, _embedded: { ancestor: [{ _direction: 'IN', _baseType: 'RELATIONSHIP', _links: { explore: [{ href: '/dashboard/explore/V315616?keyspace=pokemon&offsetEmbedded=0&limitEmbedded=5' }], self: { href: '/kb/concept/V315616?keyspace=pokemon&offsetEmbedded=0&limitEmbedded=5' } }, _embedded: { ancestor: [{ _direction: 'OUT', _baseType: 'ENTITY', _links: { explore: [{ href: '/dashboard/explore/V16488?keyspace=pokemon&offsetEmbedded=0&limitEmbedded=5' }], self: { href: '/kb/concept/V16488?keyspace=pokemon&offsetEmbedded=0&limitEmbedded=5' } }, _type: 'pokemon', _id: 'V16488' }], descendant: [{ _direction: 'OUT', _baseType: 'ENTITY', _links: { explore: [{ href: '/dashboard/explore/V786464?keyspace=pokemon&offsetEmbedded=0&limitEmbedded=5' }], self: { href: '/kb/concept/V786464?keyspace=pokemon&offsetEmbedded=0&limitEmbedded=5' } }, _type: 'pokemon', _id: 'V786464' }] }, _type: 'evolution', _id: 'V315616' }], 'pokemon-with-type': [{ _direction: 'IN', _baseType: 'RELATIONSHIP', _links: { explore: [{ href: '/dashboard/explore/V377024?keyspace=pokemon&offsetEmbedded=0&limitEmbedded=5' }], self: { href: '/kb/concept/V377024?keyspace=pokemon&offsetEmbedded=0&limitEmbedded=5' } }, _embedded: { 'type-of-pokemon': [{ _direction: 'OUT', _baseType: 'ENTITY', _links: { explore: [{ href: '/dashboard/explore/V372928?keyspace=pokemon&offsetEmbedded=0&limitEmbedded=5' }], self: { href: '/kb/concept/V372928?keyspace=pokemon&offsetEmbedded=0&limitEmbedded=5' } }, _type: 'pokemon-type', _id: 'V372928' }], 'pokemon-with-type': [{ _direction: 'OUT', _baseType: 'ENTITY', _links: { explore: [{ href: '/dashboard/explore/V16488?keyspace=pokemon&offsetEmbedded=0&limitEmbedded=5' }], self: { href: '/kb/concept/V16488?keyspace=pokemon&offsetEmbedded=0&limitEmbedded=5' } }, _type: 'pokemon', _id: 'V16488' }] }, _type: 'has-type', _id: 'V377024' }] }, _type: 'pokemon', _id: 'V16488' } }];

export const HALParserTestResponse1 = [{ x: {
  _baseType: 'ENTITY',
  _links: {
    explore: [{ href: '/dashboard/explore/984997984?keyspace=snb&offsetEmbedded=0&limitEmbedded=5' }],
    self: { href: '/graph/concept/984997984?keyspace=snb&offsetEmbedded=10&limitEmbedded=5' } },
  _embedded: {
    product: [
      {
        _direction: 'IN',
        _baseType: 'RELATION',
        _links: {
          explore: [{ href: '/dashboard/explore/3396772072?keyspace=snb&offsetEmbedded=0&limitEmbedded=5' }],
          self: { href: '/graph/concept/3396772072?keyspace=snb&offsetEmbedded=0&limitEmbedded=5' } },
        _embedded: {
          product: [
            {
              _direction: 'OUT',
              _baseType: 'ENTITY',
              _links: {
                explore: [{ href: '/dashboard/explore/984997984?keyspace=snb&offsetEmbedded=0&limitEmbedded=5' }],
                self: { href: '/graph/concept/984997984?keyspace=snb&offsetEmbedded=0&limitEmbedded=5' } },
              _type: 'post',
              _id: '984997984',
            },
          ],
          creator: [
            { _direction: 'OUT',
              _baseType: 'ENTITY',
              _links: {
                explore: [{ href: '/dashboard/explore/52064336?keyspace=snb&offsetEmbedded=0&limitEmbedded=5' }],
                self: { href: '/graph/concept/52064336?keyspace=snb&offsetEmbedded=0&limitEmbedded=5' } },
              _type: 'person',
              _id: '52064336',
            }] },
        _type: 'has-creator',
        _id: '3396772072',
      },
    ],
    original: [
      {
        _direction: 'IN',
        _baseType: 'RELATION',
        _links: {
          explore: [{ href: '/dashboard/explore/2285097056?keyspace=snb&offsetEmbedded=0&limitEmbedded=5' }],
          self: { href: '/graph/concept/2285097056?keyspace=snb&offsetEmbedded=0&limitEmbedded=5' } },
        _embedded: {
          original: [
            {
              _direction: 'OUT',
              _baseType: 'ENTITY',
              _links: {
                explore: [{ href: '/dashboard/explore/984997984?keyspace=snb&offsetEmbedded=0&limitEmbedded=5' }],
                self: {
                  href: '/graph/concept/984997984?keyspace=snb&offsetEmbedded=0&limitEmbedded=5' } },
              _type: 'post',
              _id: '984997984',
            }],
          reply: [
            {
              _direction: 'OUT',
              _baseType: 'ENTITY',
              _links: {
                explore: [{ href: '/dashboard/explore/35004512?keyspace=snb&offsetEmbedded=0&limitEmbedded=5' }],
                self: { href: '/graph/concept/35004512?keyspace=snb&offsetEmbedded=0&limitEmbedded=5' } },
              _type: 'comment',
              _id: '35004512',
            }] },
        _type: 'reply-of',
        _id: '2285097056',
      },
    ],
  },
  _type: 'post',
  _id: '984997984',
} }];

export const HALParserTestResponseReflexive = [{ x: {
  _baseType: 'RELATION',
  _links: {
    explore: [
      {
        href: '/dashboard/explore/122884320?keyspace=yoshi&offsetEmbedded=0&limitEmbedded=5',
      },
    ],
    self: {
      href: '/graph/concept/122884320?keyspace=yoshi&offsetEmbedded=0&limitEmbedded=5',
    },
  },
  _embedded: {
    pobj: [
      {
        _direction: 'OUT',
        _baseType: 'ENTITY',
        _links: {
          explore: [
            {
              href: '/dashboard/explore/24712?keyspace=yoshi&offsetEmbeddeåd=0&limitEmbedded=5',
            },
          ],
          self: {
            href: '/graph/concept/24712?keyspace=yoshi&offsetEmbedded=0&limitEmbedded=5',
          },
        },
        _type: 'word',
        _id: '24712',
      },
    ],
    prep: [
      {
        _direction: 'OUT',
        _baseType: 'ENTITY',
        _links: {
          explore: [
            {
              href: '/dashboard/explore/24712?keyspace=yoshi&offsetEmbedded=0&limitEmbedded=5',
            },
          ],
          self: {
            href: '/graph/concept/24712?keyspace=yoshi&offsetEmbedded=0&limitEmbedded=5',
          },
        },
        _type: 'word',
        _id: '24712',
      },
    ],
  },
  _type: 'pair',
  _id: '122884320',
} }];
