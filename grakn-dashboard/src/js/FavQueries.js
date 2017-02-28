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
import User from './User';


// Default constant values
const QUERIES_LS_KEY = 'fav_queries';


export default {
  getFavQueries() {
    const queries = localStorage.getItem(QUERIES_LS_KEY);
    const currentKeyspace = User.getCurrentKeySpace();

    if (queries === null) {
      localStorage.setItem(QUERIES_LS_KEY, JSON.stringify({ [currentKeyspace]: {} }));
      return {};
    }

    const queriesObject = JSON.parse(queries);
    // If there is not object associated to the current keyspace we return empty object
    if (!(currentKeyspace in queriesObject)) {
      return {};
    }
    return queriesObject[currentKeyspace];
  },
  addFavQuery(queryName, queryValue) {
    const queries = this.getFavQueries();

    queries[queryName] = queryValue;
    this.setFavQueries(queries);
  },
  removeFavQuery(queryName) {
    const queries = this.getFavQueries();
    delete queries[queryName];
    this.setFavQueries(queries);
  },
  setFavQueries(queriesParam) {
    const queries = JSON.parse(localStorage.getItem(QUERIES_LS_KEY));
    Object.assign(queries, { [User.getCurrentKeySpace()]: queriesParam });
    localStorage.setItem(QUERIES_LS_KEY, JSON.stringify(queries));
  },
};
