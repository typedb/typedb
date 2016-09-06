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

package io.mindmaps.engine.controller;


import io.mindmaps.constants.ErrorMessage;
import io.mindmaps.constants.RESTUtil;
import io.mindmaps.engine.util.ConfigProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static spark.Spark.before;
import static spark.Spark.get;


/**
 * REST controller used by MindmapsClient to retrieve graph configuration for a given graph name.
 */

public class GraphFactoryController {
    private final Logger LOG = LoggerFactory.getLogger(GraphFactoryController.class);

    public GraphFactoryController() {
        ConfigProperties prop = ConfigProperties.getInstance();

        get(RESTUtil.WebPath.GRAPH_FACTORY_URI, (req, res) -> {
            String graphConfig = req.queryParams(RESTUtil.Request.GRAPH_CONFIG_PARAM);

            try {
                if (graphConfig == null) {
                    graphConfig = ConfigProperties.GRAPH_CONFIG_PROPERTY;
                } else {
                    switch (graphConfig) {
                        case RESTUtil.GraphConfig.DEFAULT:
                            graphConfig = ConfigProperties.GRAPH_CONFIG_PROPERTY;
                            break;
                        case RESTUtil.GraphConfig.BATCH:
                            graphConfig = ConfigProperties.GRAPH_BATCH_CONFIG_PROPERTY;
                            break;
                        case RESTUtil.GraphConfig.COMPUTER:
                            graphConfig = ConfigProperties.GRAPH_COMPUTER_CONFIG_PROPERTY;
                            break;
                    }
                }
                return new String(Files.readAllBytes(Paths.get(prop.getPath(graphConfig))));
            } catch (IOException e) {
                LOG.error(ErrorMessage.NO_CONFIG_FILE.getMessage(prop.getPath(graphConfig)));
                res.status(500);
                return ErrorMessage.NO_CONFIG_FILE.getMessage(prop.getPath(graphConfig));
            }
        });

    }
}
