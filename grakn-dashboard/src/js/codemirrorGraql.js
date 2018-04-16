/*-
 * #%L
 * grakn-dashboard
 * %%
 * Copyright (C) 2016 - 2018 Grakn Labs Ltd
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */
import CodeMirror from 'codemirror';

CodeMirror.defineSimpleMode('graql', {
  // The start state contains the rules that are intially used
  start: [
    { regex: /#.*/, token: 'comment' },
    { regex: /".*?"/, token: 'string' },
    { regex: /(match|insert|delete|select|isa|sub|plays|relates|datatype|is-abstract|has|value|id|of|limit|offset|order|by|compute|from|to|in|aggregate|label|get)(?![-a-zA-Z_0-9])/, // eslint-disable-line max-len
      token: 'keyword' },
    { regex: /true|false/, token: 'number' },
    { regex: /\$[-a-zA-Z_0-9]+/, token: 'variable' },
    { regex: /[-a-zA-Z_][-a-zA-Z_0-9]*/, token: 'identifier' },
    { regex: /[0-9]+(\.[0-9][0-9]*)?/, token: 'number' },
    { regex: /=|!=|>|<|>=|<=|contains|regex/, token: 'operator' },
  ],
  comment: [],
  // The meta property contains global information about the mode. It
  // can contain properties like lineComment, which are supported by
  // all modes, and also directives like dontIndentStates, which are
  // specific to simple modes.
  meta: {
    dontIndentStates: ['comment'],
    lineComment: '#',
  },
});
