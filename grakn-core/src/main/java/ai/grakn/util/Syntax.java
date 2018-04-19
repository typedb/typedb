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
public class Syntax {

    public static final String MATCH = "match";


    // Analytics Syntax
    public static final String COMPUTE = "compute";

    /**
     * Graql Compute syntax keyword
     */
    public static class Compute {
        public static final String COUNT        = "count";
        public static final String MIN          = "min";
        public static final String MAX          = "max";
        public static final String MEDIAN       = "median";
        public static final String MEAN         = "mean";
        public static final String STD          = "std";
        public static final String SUM          = "sum";
        public static final String PATH         = "path";
        public static final String PATHS        = "paths";
        public static final String CENTRALITY   = "centrality";
        public static final String CLUSTER      = "cluster";
    }
}
