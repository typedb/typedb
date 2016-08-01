/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.api;


import io.mindmaps.util.ConfigProperties;
import io.mindmaps.util.RESTUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static spark.Spark.get;


/**
 * REST controller used by MindmapsClient to retrieve graph configuration for a given graph name.
 */

public class GraphFactoryController {
    private final Logger LOG = LoggerFactory.getLogger(GraphFactoryController.class);

    private final String graphConfig;

    public GraphFactoryController() {

        graphConfig = ConfigProperties.getInstance().getProperty(ConfigProperties.GRAPH_CONFIG_PROPERTY);

        get(RESTUtil.WebPath.GRAPH_FACTORY_URI, (req, res) -> {
            try {
                return new String(Files.readAllBytes(Paths.get(graphConfig)));
            } catch (IOException e) {
                LOG.error("Cannot find config file [" + graphConfig + "]", e);
                throw new IOException("Cannot find config file [" + graphConfig + "]");
            }
        });

    }
}
