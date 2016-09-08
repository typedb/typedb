package io.mindmaps.util;

public class REST {

    public static class WebPath{
        public static final String IMPORT_DATA_URI =  "/import/batch/data";
        public static final String IMPORT_ONTOLOGY_URI = "/import/ontology";

        public static final String GRAPH_FACTORY_URI = "/graph_factory";

        public static final String META_TYPE_INSTANCES_URI = "/shell/metaTypeInstances";
        public static final String MATCH_QUERY_URI = "/shell/match";
        public static final String GRAPH_MATCH_QUERY_URI = "/graph/match";

        public static final String HOME_URI = "/";

        public static final String CONCEPT_BY_ID_URI = "/graph/concept/" ;

        public static final String COMMIT_LOG_URI = "/commit_log";

        public static final String NEW_TRANSACTION_URI = "/transaction/new";
        public static final String TRANSACTION_STATUS_URI = "/transaction/status/";
        public static final String LOADER_STATE_URI = "/transaction/loaderState";
        public static final String GET_STATUS_URI = "/status";

        public static final String REMOTE_SHELL_URI = "/shell/remote";
    }

    public static class Request{
        public static final String PATH_FIELD = "path";
        public static final String QUERY_FIELD = "query";
        public static final String VALUE_FIELD = "value";
        public static final String ID_PARAMETER = ":id";
        public static final String GRAPH_NAME_PARAM = "graphName";
        public static final String GRAPH_CONFIG_PARAM = "graphConfig";
        public static final String UUID_PARAMETER = ":uuid";
    }

    public static class GraphConfig{
        public static final String DEFAULT = "default";
        public static final String BATCH = "batch";
        public static final String COMPUTER = "computer";
    }

    public static class HttpConn{
        public static final String INSERT_PREFIX = "insert ";
        public static final int HTTP_TRANSACTION_CREATED = 201;
        public static final String UTF8 = "UTF8";
        public static final String CONTENT_LENGTH = "Content-Length";
        public static final String CONTENT_TYPE = "Content-Type";
        public static final String POST_METHOD = "POST";
        public static final String APPLICATION_POST_TYPE = "application/POST";
    }

    public static class Response{

        public static final String ENTITIES_JSON_FIELD = "entities";
        public static final String ROLES_JSON_FIELD = "roles";
        public static final String RELATIONS_JSON_FIELD = "relations";
        public static final String RESOURCES_JSON_FIELD = "resources";

    }

    public static class RemoteShell {
        public static final String ACTION = "action";
        public static final String ACTION_NAMESPACE = "namespace";
        public static final String ACTION_QUERY = "query";
        public static final String ACTION_QUERY_END = "queryEnd";
        public static final String ACTION_COMMIT = "commit";
        public static final String ACTION_AUTOCOMPLETE = "autocomplete";

        public static final String NAMESPACE = "namespace";
        public static final String QUERY = "query";
        public static final String QUERY_LINES = "lines";
        public static final String AUTOCOMPLETE_CANDIDATES = "candidates";
        public static final String AUTOCOMPLETE_CURSOR = "cursor";
        public static final String ERROR = "error";
    }
}
