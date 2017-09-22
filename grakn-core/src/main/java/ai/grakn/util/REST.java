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

import static ai.grakn.util.REST.Request.ENTITY_CONCEPT_ID_PARAMETER;

/**
 * Class containing strings describing the REST API, including URIs and fields.
 *
 * @author Marco Scoppetta
 */
public class REST {

    /**
     * Class containing URIs to REST endpoints.
     */
    public static class WebPath{

        public static final String COMMIT_LOG_URI = "/commit_log";

        public static final String REMOTE_SHELL_URI = "/shell/remote";

        /**
         * URIs to visualiser controller
         */
        public static class KB {
            @Deprecated
            public static final String GRAQL = "/kb/graql";
            public static final String ANY_GRAQL = "/kb/graql/execute";
        }

        /**
         * URIs to Tasks Controller endpoints
         */
        public static class Tasks {
            public static final String TASKS = "/tasks";
            public static final String GET = "/tasks/:id";
            public static final String STOP = "/tasks/:id/stop";
        }

        /**
         * URIs to System Controller endpoints
         */
        public static class System {
            public static final String DELETE_KEYSPACE = "/deleteKeyspace";
            public static final String INITIALISE = "/initialise";
            public static final String STATUS = "/status";
            public static final String CONFIGURATION = "/configuration";
            public static final String METRICS = "/metrics";
            public static final String KEYSPACES = "/keyspaces";
        }

        /**
         * URIs to concept controller endpoints
         */
        public static class Concept {
            public static final String CONCEPT = "/kb/concept/";
            public static final String SCHEMA = "/kb/schema";
        }

        /**
         * URIs to api endpoints
         */
        public static class Api {
            public static final String API_PREFIX = "/api";

            public static final String ATTRIBUTE_TYPE = API_PREFIX + "/attributeType";
            public static final String ENTITY_TYPE = API_PREFIX + "/entityType";
            public static final String RELATIONSHIP_TYPE = API_PREFIX + "/relationshipType";
            public static final String ROLE = API_PREFIX + "/role";
            public static final String RULE = API_PREFIX + "/rule";

            public static final String ENTITY_TYPE_ATTRIBUTE_TYPE_ASSIGNMENT = API_PREFIX + "/entityType/" + Request.ENTITY_TYPE_LABEL_PARAMETER +
                "/attributeType/" + Request.ATTRIBUTE_TYPE_LABEL_PARAMETER;

            public static final String ENTITY_ATTRIBUTE_ASSIGNMENT = API_PREFIX + "/entity/" + ENTITY_CONCEPT_ID_PARAMETER +
                "/attribute/" + Request.ATTRIBUTE_CONCEPT_ID_PARAMETER;

            public static final String RELATIONSHIP_ENTITY_ROLE_ASSIGNMENT = API_PREFIX + "/relationship/" + Request.RELATIONSHIP_CONCEPT_ID_PARAMETER +
                "/entity/" + ENTITY_CONCEPT_ID_PARAMETER +
                "/role/" + Request.ROLE_LABEL_PARAMETER;

        }

        /**
         * URIs to dashboard controller endpoints
         */
        public static class Dashboard {
            public static final String TYPES = "/dashboard/types/";
            public static final String EXPLORE = "/dashboard/explore/";
            public static final String EXPLAIN = "/dashboard/explain";
        }

        public static final String NEW_SESSION_URI="/auth/session/";
        public static final String IS_PASSWORD_PROTECTED_URI="/auth/enabled/";

        public static final String ALL_USERS = "/user/all";
        public static final String ONE_USER = "/user/one";
    }

    /**
     * Class containing request fields and content types.
     */
    public static class Request {
        // Attributes set and used on the server-side
        public static final String USER_ATTR = "user";
        
        // Request parameters
        public static final String ID_PARAMETER = ":id";
        public static final String KEYSPACE_PARAM = "keyspace";
        public static final String TASK_STATUS_PARAMETER = "status";
        public static final String TASK_CLASS_NAME_PARAMETER = "className";
        public static final String TASK_CREATOR_PARAMETER = "creator";
        public static final String TASK_RUN_AT_PARAMETER = "runAt";
        public static final String TASK_PRIORITY_PARAMETER = "priority";
        public static final String TASK_RUN_INTERVAL_PARAMETER = "interval";
        public static final String TASK_RUN_WAIT_PARAMETER = "wait";
        public static final String TASK_LOADER_MUTATIONS = "mutations";
        public static final String BATCH_NUMBER = "batchNumber";
        public static final String LIMIT_PARAM = "limit";
        public static final String OFFSET_PARAM = "offset";
        public static final String TASKS_PARAM = "tasks";
        public static final String CONFIGURATION_PARAM = "configuration";
        public static final String KEYSPACE = "keyspace";
        public static final String FORMAT = "format";
        public static final String UUID_PARAMETER = "uuid";

        // URL parameters for API endpoints
        public static final String ATTRIBUTE_TYPE_LABEL_PARAMETER = ":attributeTypeLabel";
        public static final String ENTITY_TYPE_LABEL_PARAMETER = ":entityTypeLabel";
        public static final String RELATIONSHIP_TYPE_LABEL_PARAMETER = ":relationshipTypeLabel";
        public static final String ROLE_LABEL_PARAMETER = ":roleLabel";
        public static final String RULE_LABEL_PARAMETER = ":ruleLabel";
        public static final String CONCEPT_ID_JSON_FIELD = "conceptId";
        public static final String ENTITY_CONCEPT_ID_PARAMETER = ":entityConceptId";
        public static final String ATTRIBUTE_CONCEPT_ID_PARAMETER = ":attributeConceptId";
        public static final String RELATIONSHIP_CONCEPT_ID_PARAMETER = ":relationshipConceptId";

        // json fields for API endpoints
        public static final String VALUE_JSON_FIELD = "value";
        public static final String ENTITY_OBJECT_JSON_FIELD = "entity";
        public static final String ATTRIBUTE_OBJECT_JSON_FIELD = "attribute";
        public static final String RELATIONSHIP_OBJECT_JSON_FIELD = "relationship";
        public static final String RELATIONSHIP_TYPE_OBJECT_JSON_FIELD = "relationshipType";
        public static final String ATTRIBUTE_TYPE_OBJECT_JSON_FIELD = "attributeType";
        public static final String ROLE_OBJECT_JSON_FIELD = "role";
        public static final String RULE_OBJECT_JSON_FIELD = "rule";
        public static final String ENTITY_TYPE_OBJECT_JSON_FIELD = "entityType";
        public static final String LABEL_JSON_FIELD = "label";
        public static final String TYPE_JSON_FIELD = "type";
        public static final String ROLE_ARRAY_JSON_FIELD = "roles";
        public static final String WHEN_JSON_FIELD = "when";
        public static final String THEN_JSON_FIELD = "then";

        //Commit Logs
        public static final String COMMIT_LOG_FIXING = "concepts-to-fix";
        public static final String COMMIT_LOG_COUNTING = "types-with-new-counts";
        public static final String COMMIT_LOG_CONCEPT_ID = "concept-id";
        public static final String COMMIT_LOG_SHARDING_COUNT = "sharding-count";

        /**
         * Concept controller request parameters
         */
        public static final class Concept {
            public static final String LIMIT_EMBEDDED = "limitEmbedded";
            public static final String OFFSET_EMBEDDED = "offsetEmbedded";
        }

        /**
         * Graql controller request parameters
         */
        public static final class Graql {
            public static final String QUERY = "query";
            public static final String INFER = "infer";
            public static final String MATERIALISE = "materialise";
            public static final String LIMIT_EMBEDDED = "limitEmbedded";
        }
    }

    /**
     * Class listing possible knowledge base configuration options.
     */
    public static class KBConfig {
        public static final String DEFAULT = "default";
        public static final String COMPUTER = "computer";
    }

    /**
     * Class listing various HTTP connection strings.
     */
    public static class HttpConn{
        public static final String POST_METHOD = "POST";
        public static final String DELETE_METHOD = "DELETE";
        public static final String GET_METHOD = "GET";
    }

    /**
     * Class listing various strings found in responses from the REST API.
     */
    public static class Response{

        public static final String EXCEPTION = "exception";

        /**
         * Response content types
         */
        public static class ContentType {
            public static final String APPLICATION_JSON_GRAQL = "application/graql+json";
            public static final String APPLICATION_JSON = "application/json";
            public static final String APPLICATION_TEXT = "application/text";
            public static final String APPLICATION_HAL ="application/hal+json";
            public static final String APPLICATION_ALL ="*/*";
        }

        /**
         * Graql controller response fields
         */
        public static class Graql {
            public static final String RESPONSE = "response";
            public static final String IDENTIFIER = "identifier";
            public static final String ORIGINAL_QUERY = "originalQuery";
        }

        /**
         *  Metatypes Json object fields
         */

        public static class Json {
            public static final String ENTITIES_JSON_FIELD = "entities";
            public static final String ROLES_JSON_FIELD = "roles";
            public static final String RELATIONSHIPS_JSON_FIELD = "relationships";
            public static final String ATTRIBUTES_JSON_FIELD = "attributes";
        }

        /**
         * Json fields used to describe tasks
         */
        public static class Task {
            public static final String STACK_TRACE = "stackTrace";
            public static final String ID = "id";
            public static final String STATUS = "status";
        }
    }

    /**
     * Class listing various strings used in the JSON messages sent using websockets for the remote Graql shell.
     */
    public static class RemoteShell {
        public static final String ACTION = "action";
        public static final String ACTION_INIT = "init";
        public static final String ACTION_QUERY = "query";
        public static final String ACTION_END = "end";
        public static final String ACTION_ERROR = "error";
        public static final String ACTION_COMMIT = "commit";
        public static final String ACTION_ROLLBACK = "rollback";
        public static final String ACTION_CLEAN = "clean";
        public static final String ACTION_PING = "ping";
        public static final String ACTION_TYPES = "types";
        public static final String ACTION_DISPLAY = "display";

        public static final String USERNAME = "username";
        public static final String PASSWORD = "password";
        public static final String KEYSPACE = "keyspace";
        public static final String OUTPUT_FORMAT = "outputFormat";
        public static final String INFER = "infer";
        public static final String MATERIALISE = "materialise";
        public static final String QUERY = "query";
        public static final String QUERY_RESULT = "result";
        public static final String ERROR = "error";
        public static final String TYPES = "types";
        public static final String DISPLAY = "display";
    }
}
