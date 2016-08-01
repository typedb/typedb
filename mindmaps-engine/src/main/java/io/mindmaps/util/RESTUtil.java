package io.mindmaps.util;

public class RESTUtil {

    public static class WebPath{
        public static final String IMPORT_DATA_URI =  "/importDataFromFile/";
        public static final String IMPORT_ONTOLOGY_URI = "/importOntologyFromFile/";

        public static final String GRAPH_FACTORY_URI = "/graph_factory";

        public static final String META_TYPE_INSTANCES_URI = "/metaTypeInstances";
        public static final String MATCH_QUERY_URI = "/match";
        public static final String HOME_URI = "/";

        public static final String CONCEPT_BY_ID_URI = "/concept/:id" ;
        public static final String CONCEPTS_BY_VALUE_URI = "/concepts";

    }

    public static class Request{
        public static final String PATH_FIELD = "path";
        public static final String QUERY_FIELD = "query";
        public static final String VALUE_FIELD = "value";
        public static final String ID_PARAMETER = ":id";


    }

    public static class Response{
        public static final String ENTITIES_JSON_FIELD = "entities";
        public static final String ROLES_JSON_FIELD = "roles";
        public static final String RELATIONS_JSON_FIELD = "relations";
        public static final String RESOURCES_JSON_FIELD = "resources";

    }
}
