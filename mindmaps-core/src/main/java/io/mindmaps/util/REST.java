package io.mindmaps.util;

public class REST {

    public static class WebPath{
        public static final String IMPORT_DATA_URI =  "/import/batch/data";
        public static final String IMPORT_DISTRIBUTED_URI =  "/import/distribute/data";

        public static final String GRAPH_FACTORY_URI = "/graph_factory";

        public static final String META_TYPE_INSTANCES_URI = "/shell/metaTypeInstances";
        public static final String MATCH_QUERY_URI = "/shell/match";
        public static final String GRAPH_MATCH_QUERY_URI = "/graph/match";

        public static final String CONCEPT_BY_ID_URI = "/graph/concept/" ;
        public static final String CONCEPT_BY_ID_ONTOLOGY_URI = "/graph/concept/ontology/" ;


        public static final String COMMIT_LOG_URI = "/commit_log";

        public static final String NEW_TRANSACTION_URI = "/transaction/new";
        public static final String TRANSACTION_STATUS_URI = "/transaction/status/";
        public static final String LOADER_STATE_URI = "/transaction/loaderState";
        public static final String GET_STATUS_CONFIG_URI = "/status/config";


        public static final String REMOTE_SHELL_URI = "/shell/remote";


        public static final String ALL_BACKGROUND_TASKS_URI = "/backgroundtasks/all";
        public static final String BACKGROUND_TASKS_BY_STATUS = "/backgroundtasks/tasks/";
        public static final String BACKGROUND_TASK_STATUS ="/backgroundtasks/task/";
    }

    public static class Request{
        public static final String PATH_FIELD = "path";
        public static final String QUERY_FIELD = "query";
        public static final String VALUE_FIELD = "value";
        public static final String ID_PARAMETER = ":id";
        public static final String GRAPH_NAME_PARAM = "graphName";
        public static final String GRAPH_CONFIG_PARAM = "graphConfig";
        public static final String UUID_PARAMETER = ":uuid";
        public static final String TASK_STATUS_PARAMETER = ":status";
        public static final String TASK_PAUSE = "/pause";
        public static final String TASK_RESUME = "/resume";
        public static final String TASK_STOP = "/stop";
        public static final String TASK_RESTART = "/restart";
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
        public static final String DELETE_METHOD = "DELETE";
        public static final String GET_METHOD = "GET";
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
        public static final String ACTION_INIT = "init";
        public static final String ACTION_QUERY = "query";
        public static final String ACTION_QUERY_END = "queryEnd";
        public static final String ACTION_ERROR = "error";
        public static final String ACTION_QUERY_ABORT = "queryAbort";
        public static final String ACTION_COMMIT = "commit";
        public static final String ACTION_ROLLBACK = "rollback";
        public static final String ACTION_AUTOCOMPLETE = "autocomplete";
        public static final String ACTION_PING = "ping";

        public static final String KEYSPACE = "keyspace";
        public static final String OUTPUT_FORMAT = "outputFormat";
        public static final String QUERY = "query";
        public static final String QUERY_RESULT = "result";
        public static final String AUTOCOMPLETE_CANDIDATES = "candidates";
        public static final String AUTOCOMPLETE_CURSOR = "cursor";
        public static final String ERROR = "error";
    }
}
