/*
 *  GRAKN.AI - THE KNOWLEDGE GRAPH
 *  Copyright (C) 2018 Grakn Labs Ltd
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package sharedconfig;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 *
 */
public class Configs {

    public static String GRAKN_URI = "localhost:48555";

    public static final Path GRAKN_PATH  = Paths.get(".").toAbsolutePath();
    public static final Path CONF_PATH = GRAKN_PATH.resolve(Paths.get("grakn-benchmark", "benchmark-runner", "conf"));


    /**
     *
     */
    public class ElasticSearchConfig {

        public static final String INDEX_TEMPLATE_NAME = "grakn-benchmark-index-template";
        public static final String INDEX_TEMPLATE =
            "{"+
                "\"index_patterns\": [\"benchmarking:span-*\"],"+
                "\"settings\": {"+
                "    \"mapper\": {"+
                "        \"dynamic\": true"+
                "    }"+
                "},"+
                "\"mappings\": {"+
                "    \"_default_\": {"+
                "        \"dynamic_templates\": ["+
                "            {"+
                "              \"integers\": {"+
                "                \"match_mapping_type\": \"long\","+
                "                \"mapping\": {"+
                "                  \"type\": \"long\""+
                "                }"+
                "              }"+
                "            }"+
                "        ]"+
                "    },"+
                "    \"span\": {"+
                "        \"properties\": {"+
                "            \"tags.concepts\": {"+
                "                \"type\": \"long\""+
                "            },"+
                "            \"traceId\": {"+
                "                \"type\": \"keyword\", "+
                "                \"norms\": \"false\""+
                "            },"+
                "            \"name\": {"+
                "                \"type\": \"keyword\","+
                "                \"norms\": \"false\""+
                "            },"+
                "            \"annotations\": {"+
                "                \"type\": \"object\","+
                "                \"enabled\": \"true\""+
                "            },"+
                "            \"tags\": {"+
                "                \"enabled\": true,"+
                "                \"dynamic\": true,"+
                "                \"type\": \"object\""+
                "            }"+
                "        }"+
                "    }"+
                "}" +
            "}";
    }





}
