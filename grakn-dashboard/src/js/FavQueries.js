// Default constant values
const QUERIES_LS_KEY = 'fav_queries';

export default {
  getFavQueries() {
    const queries = localStorage.getItem(QUERIES_LS_KEY);
    if (queries === null) {
      localStorage.setItem(QUERIES_LS_KEY, '{}');
      return {};
    }
    return JSON.parse(queries);
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
  setFavQueries(queriesObject) {
    localStorage.setItem(QUERIES_LS_KEY, JSON.stringify(queriesObject));
  },
};
