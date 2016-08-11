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

import io.mindmaps.loader.Loader;
import io.mindmaps.util.ConfigProperties;
import io.mindmaps.constants.RESTUtil;

import java.util.UUID;

import static spark.Spark.get;
import static spark.Spark.post;

public class TransactionController {

    Loader loader;
    String defaultGraphName = ConfigProperties.getInstance().getProperty(ConfigProperties.DEFAULT_GRAPH_NAME_PROPERTY);

    public TransactionController() {

        loader = Loader.getInstance();

        post(RESTUtil.WebPath.NEW_TRANSACTION_URI, (req, res) -> {

            String currentGraphName = req.queryParams(RESTUtil.Request.GRAPH_NAME_PARAM);
            if (currentGraphName == null) currentGraphName = defaultGraphName;
            UUID uuid = loader.addJob(currentGraphName, req.body());

            if (uuid != null) {
                res.status(201);
                return uuid.toString();
            } else {
                res.status(405);
                return "Error";
            }
        });

        get(RESTUtil.WebPath.TRANSACTION_STATUS_URI, (req, res) -> {
            try {
                return loader.getStatus(UUID.fromString(req.params(RESTUtil.Request.UUID_PARAMETER))).toString();
            } catch (Exception e) {
                e.printStackTrace();
                res.status(400);
                return e.getMessage();
            }
        });

    }
}