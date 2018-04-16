/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.util;

/**
 * Class containing strings describing the REST API, including URIs and fields.
 *
 * @author Marco Scoppetta
 */
public class REST {

    public static String resolveTemplate(String pathTemplate, String... pathParams) {
        // e.g. `/kb/:keyspace/commit_log` -> `/kb/%s/commit_log`
        String format = pathTemplate.replaceAll(":[^/]+", "%s");
        return String.format(format, (Object[]) pathParams);
    }

    /**
     * e.g. /kb/:keyspace/graql -> /kb/{keyspace}/graql
     */
    public static String reformatTemplate(String pathTemplate) {
        return pathTemplate.replaceAll(":([^/]+)", "{$1}");
    }

    /**
     * Class containing URIs to REST endpoints.
     */
    public static class WebPath{

        public static final String ROOT = "/";

        /**
         * Keyspace Specific Operations
         */
        public static final String KB = "/kb";
        public static final String KB_KEYSPACE = "/kb/:keyspace";
        public static final String KEYSPACE_TYPE = "/kb/:keyspace/type";
        public static final String KEYSPACE_ROLE = "/kb/:keyspace/role";
        public static final String KEYSPACE_RULE = "/kb/:keyspace/rule";
        public static final String KEYSPACE_GRAQL = "/kb/:keyspace/graql";
        public static final String KEYSPACE_EXPLAIN = "/kb/:keyspace/explain";
        public static final String COMMIT_LOG_URI = "/kb/:keyspace/commit_log";

        /**
         * Concept Specific operations
         */
        public static final String CONCEPT_LINK = "/kb/:keyspace/:base-type/:id";
        public static final String CONCEPT_ID = "/kb/:keyspace/concept/:id";
        public static final String CONCEPT_ATTRIBUTES = "/kb/:keyspace/concept/:id/attributes";
        public static final String CONCEPT_KEYS = "/kb/:keyspace/concept/:id/keys";
        public static final String CONCEPT_RELATIONSHIPS = "/kb/:keyspace/concept/:id/relationships";

        public static final String TYPE_LABEL = "/kb/:keyspace/type/:label";
        public static final String TYPE_SUBS = "/kb/:keyspace/type/:label/subs";
        public static final String TYPE_PLAYS = "/kb/:keyspace/type/:label/plays";
        public static final String TYPE_ATTRIBUTES = "/kb/:keyspace/type/:label/attributes";
        public static final String TYPE_KEYS = "/kb/:keyspace/type/:label/keys";
        public static final String TYPE_INSTANCES = "/kb/:keyspace/type/:label/instances";

        public static final String RULE_LABEL = "/kb/:keyspace/rule/:label";
        public static final String RULE_SUBS = "/kb/:keyspace/rule/:label/subs";

        public static final String ROLE_LABEL = "/kb/:keyspace/role/:label";
        public static final String ROLE_SUBS = "/kb/:keyspace/role/:label/subs";

        /**
         * URIs to System Controller endpoints
         */
        public static final String STATUS = "/status";
        public static final String VERSION = "/version";
        public static final String METRICS = "/metrics";
    }

    /**
     * Class containing request fields and content types.
     */
    public static class Request {
        // Request parameters
        public static final String LABEL_PARAMETER = ":label";
        public static final String ID_PARAMETER = ":id";
        public static final String KEYSPACE_PARAM = "keyspace";
        public static final String LIMIT_PARAMETER = "limit";
        public static final String OFFSET_PARAMETER = "offset";
        public static final String FORMAT = "format";

        /**
         * Graql controller request parameters
         */
        public static final class Graql {
            public static final String QUERY = "query";
            public static final String EXECUTE_WITH_INFERENCE = "infer";
            public static final String ALLOW_MULTIPLE_QUERIES = "multi";
            public static final String TX_TYPE = "txType";
            public static final String DEFINE_ALL_VARS = "defineAllVars";
            public static final String LOADING_DATA = "loading";
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
        public static final String PUT_METHOD = "PUT";
        public static final String DELETE_METHOD = "DELETE";
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
            public static final String APPLICATION_TEXT = "application/text";
            public static final String APPLICATION_JSON = "application/json";
            public static final String APPLICATION_ALL ="*/*";
        }

    }
}
