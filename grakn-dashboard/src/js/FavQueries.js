//Default constant values
const QUERIES_LS_KEY = "fav_queries";

export default {
    getFavQueries() {
        let queries = localStorage.getItem(QUERIES_LS_KEY);
        if (queries == undefined) {
            localStorage.setItem(QUERIES_LS_KEY, "{}");
            return {};
        } else
            return JSON.parse(queries);
    },
    addFavQuery(query){
      let queries = this.getFavQueries();
      queries[query.name]=query.value;
      this.setFavQueries(queries);
    },
    setFavQueries(queriesObject){
      localStorage.setItem(QUERIES_LS_KEY,JSON.stringify(queriesObject));
    }
}
