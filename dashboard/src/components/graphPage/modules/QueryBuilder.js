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


export default {
  commonConceptsBuilder(nodeIds) {
    // Skipping 'x' since we will use it as common letter
    const alphabet = 'abcdefghijklmnopqrstuvwyz'.split('');
    const arrayLength = nodeIds.length;

    let queryString = 'match ';
    for (let i = 0; i < arrayLength; i++) {
      queryString += `$${alphabet[i]} id '${nodeIds[i]}'; `;
    }
    queryString += '\n';
    for (let i = 0; i < arrayLength; i++) {
      queryString += `($${alphabet[i]},$x);`;
    }
    queryString += '\nget $x;';
    return queryString;
  },

  shortestPathBuilder(nodeIds) {
    return `compute path from "${nodeIds[0]}", to "${nodeIds[1]}";`;
  },

  exploreRelationshipsBuilder(nodeIds) {
    return `match $x id "${nodeIds[0]}"; $y id "${nodeIds[1]}"; $r ($x, $y); get;`;
  },

};
