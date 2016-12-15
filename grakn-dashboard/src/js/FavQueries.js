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
    addFavQuery(queryName,queryValue){
      let queries = this.getFavQueries();
      queries[queryName]=queryValue;
      this.setFavQueries(queries);
    },
    setFavQueries(queriesObject){
      localStorage.setItem(QUERIES_LS_KEY,JSON.stringify(queriesObject));
    }
}
