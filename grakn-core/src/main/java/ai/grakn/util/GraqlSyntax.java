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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.util;

/**
 * Graql syntax keywords
 *
 * @author Haikal Pribadi
 */
public class GraqlSyntax {

    public static final String MATCH = "match";


    // Graql Queries
    public static final String COMPUTE = "compute";


    // Miscellaneous
    public static final String EQUAL            = "=";
    public static final String SEMICOLON        = ";";
    public static final String SPACE            = " ";
    public static final String COMMA            = ",";
    public static final String COMMA_SPACE      = ", ";
    public static final String SQUARE_OPEN      = "[";
    public static final String SQUARE_CLOSE     = "]";
    public static final String QUOTE            = "\"";

    /**
     * Graql Compute syntax keyword
     */
    public static class Compute {
        public static final String COUNT        = "setNumber";
        public static final String MIN          = "min";
        public static final String MAX          = "max";
        public static final String MEDIAN       = "median";
        public static final String MEAN         = "mean";
        public static final String STD          = "std";
        public static final String SUM          = "sum";
        public static final String PATH         = "path";
        public static final String CENTRALITY   = "centrality";
        public static final String CLUSTER      = "cluster";

        /**
         * Graql Compute conditions keyword
         */
        public static class Condition {
            public static final String FROM     = "from";
            public static final String TO       = "to";
            public static final String OF       = "of";
            public static final String IN       = "in";
            public static final String USING    = "using";
            public static final String WHERE    = "where";
        }

        /**
         * Graql Compute algorithm names
         */
        public static class Algorithm {
            public static final String DEGREE               = "degree";
            public static final String K_CORE               = "k-core";
            public static final String CONNECTED_COMPONENT  = "connected-component";
        }

        /**
         * Graql Compute argument keywords
         */
        public static class Arg {
            public static final String MIN_K    = "min-k";
            public static final String K        = "k";
            public static final String START    = "start";
            public static final String MEMBERS  = "members";
            public static final String SIZE     = "size";
        }
    }
}
