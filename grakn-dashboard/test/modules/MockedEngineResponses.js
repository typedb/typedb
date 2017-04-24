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


export const HALParserTestResponse0 = { _baseType: 'ENTITY',
  _links: { explore: [{ href: '/dashboard/explore/4128?keyspace=snb&offsetEmbedded=0&limitEmbedded=2' }], self: { href: '/graph/concept/4128?keyspace=snb&offsetEmbedded=0&limitEmbedded=2' } },
  _embedded: { isa: [{ _direction: 'OUT',
    _baseType: 'ENTITY_TYPE',
    _name: 'person',
    _links: { explore: [{ href: '/dashboard/explore/57368?keyspace=snb&offsetEmbedded=0&limitEmbedded=2' }], self: { href: '/graph/concept/57368?keyspace=snb&offsetEmbedded=0&limitEmbedded=2' } },
    _id: '57368' }] },
  _type: 'person',
  _id: '4128',
};

export const HALParserTestResponse1 = {
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
};

export const HALParserTestResponseReflexive = {
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
};
