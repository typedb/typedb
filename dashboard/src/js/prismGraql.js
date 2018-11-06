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

/**
 * Contains the Graql syntax highlighter for Prism.
 */
export default {
  comment: {
    pattern: /#.*/,
    alias: 'comment',
  },
  string: {
    pattern: /".*?"/,
    alias: 'string',
  },
  keyword: {
    pattern: /((?:(?![-a-zA-Z_0-9]).)|^|\s)(match|insert|delete|select|isa|sub|plays|relates|datatype|is-abstract|has|value|id|of|limit|offset|order|by|compute|aggregate|label|get|using|where)(?![-a-zA-Z_0-9])/, // eslint-disable-line max-len
    alias: 'keyword',
    lookbehind: true,
  },

  special: {
    pattern: /graql>|results>|\.\.\./,
  },
  variable: {
    pattern: /\$[-a-zA-Z_0-9]+/,
    alias: 'variable',
  },
  type: {
    pattern: /[-a-zA-Z_][-a-zA-Z_0-9]*/,
    alias: 'function',
  },
  number: {
    pattern: /[0-9]+(\.[0-9][0-9]*)?/,
    alias: 'number',
  },
  operator: {
    pattern: /=|!=|>|<|>=|<=|\[|\]|contains|regex/,
    alias: 'operator',
  },
};
