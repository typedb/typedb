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
