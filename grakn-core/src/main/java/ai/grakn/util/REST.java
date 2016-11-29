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

package ai.grakn.util;

public class REST {

    public static class WebPath{
        public static final String IMPORT_DATA_URI =  "/import/batch/data";
        public static final String GRAPH_FACTORY_URI = "/graph_factory";

        public static final String GRAPH_MATCH_QUERY_URI = "/graph/match";
        public static final String GRAPH_ANALYTICS_QUERY_URI = "/graph/analytics";
        public static final String GRAPH_PRE_MATERIALISE_QUERY_URI = "/graph/preMaterialiseAll";
        public static final String GRAPH_ONTOLOGY_URI = "/graph/ontology" ;
        public static final String CONCEPT_BY_ID_URI = "/graph/concept/" ;
        public static final String CONCEPT_BY_ID_ONTOLOGY_URI = "/graph/concept/ontology/" ;

        public static final String COMMIT_LOG_URI = "/commit_log";
        public static final String GET_STATUS_CONFIG_URI = "/status/config";

        public static final String REMOTE_SHELL_URI = "/shell/remote";

        public static final String ALL_TASKS_URI = "/tasks/all";
        public static final String TASKS_URI = "/tasks";
        public static final String TASKS_SCHEDULE_URI ="/tasks/schedule";
    }

    public static class Request {
        public static final String PATH_FIELD = "path";
        public static final String HOSTS_FIELD = "hosts";
        public static final String QUERY_FIELD = "query";
        public static final String ID_PARAMETER = ":id";
        public static final String KEYSPACE_PARAM = "keyspace";
        public static final String GRAPH_CONFIG_PARAM = "graphConfig";
        public static final String UUID_PARAMETER = ":uuid";
        public static final String TASK_STATUS_PARAMETER = "status";
        public static final String TASK_CLASS_NAME_PARAMETER = "className";
        public static final String TASK_CREATOR_PARAMETER = "creator";
        public static final String TASK_RUN_AT_PARAMETER = "runAt";
        public static final String TASK_RUN_INTERVAL_PARAMETER = "interval";
        public static final String TASK_CONFIGURATION_PARAMETER = "configuration";
        public static final String TASK_LOADER_INSERTS = "inserts";
        public static final String TASK_STOP = "/stop";
        public static final String LIMIT_PARAM = "limit";
        public static final String OFFSET_PARAM = "offset";
        public static final String HAL_CONTENTTYPE = "application/hal+json";
        public static final String GRAQL_CONTENTTYPE = "application/graql";
    }

    public static class GraphConfig{
        public static final String DEFAULT = "default";
        public static final String BATCH = "batch";
        public static final String COMPUTER = "computer";
    }

    public static class HttpConn{
        public static final String INSERT_PREFIX = "insert ";
        public static final int OK = 200;
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
        public static final String ACTION_END = "end";
        public static final String ACTION_ERROR = "error";
        public static final String ACTION_QUERY_ABORT = "queryAbort";
        public static final String ACTION_COMMIT = "commit";
        public static final String ACTION_ROLLBACK = "rollback";
        public static final String ACTION_PING = "ping";
        public static final String ACTION_TYPES = "types";

        public static final String KEYSPACE = "keyspace";
        public static final String OUTPUT_FORMAT = "outputFormat";
        public static final String QUERY = "query";
        public static final String QUERY_RESULT = "result";
        public static final String ERROR = "error";
        public static final String TYPES = "types";
    }
}
