import storage from '@/components/shared/PersistentStorage';
// Default constant values
const QUERIES_LS_KEY = 'fav_queries';

export default {
  getFavQueries(currentKeyspace) {
    const queries = storage.get(QUERIES_LS_KEY);

    if (queries == null) {
      storage.set(QUERIES_LS_KEY, JSON.stringify({ [currentKeyspace]: {} }));
      return {};
    }

    const queriesObject = JSON.parse(queries);
    // If there is not object associated to the current keyspace we return empty object
    if (!(currentKeyspace in queriesObject)) {
      return {};
    }
    return queriesObject[currentKeyspace];
  },
  addFavQuery(queryName, queryValue, currentKeyspace) {
    const queries = this.getFavQueries(currentKeyspace);

    queries[queryName] = queryValue;
    this.setFavQueries(queries, currentKeyspace);
  },
  removeFavQuery(queryName, currentKeyspace) {
    const queries = this.getFavQueries(currentKeyspace);
    delete queries[queryName];
    this.setFavQueries(queries, currentKeyspace);
  },
  setFavQueries(queriesParam, currentKeyspace) {
    const queries = JSON.parse(storage.get(QUERIES_LS_KEY));
    Object.assign(queries, { [currentKeyspace]: queriesParam });
    storage.set(QUERIES_LS_KEY, JSON.stringify(queries));
  },
};
