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
import io.mindmaps.core.implementation.MindmapsTransactionImpl;
import io.mindmaps.factory.GraphFactory;
import io.mindmaps.graql.api.parser.QueryParser;
import io.mindmaps.util.RESTUtil;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import java.util.stream.Collectors;

import static spark.Spark.get;
import static spark.Spark.redirect;

public class RemoteShellController {

    private final Logger LOG = LoggerFactory.getLogger(RemoteShellController.class);

    String graphName = ConfigProperties.getInstance().getProperty(ConfigProperties.DEFAULT_GRAPH_NAME_PROPERTY);

    public RemoteShellController() {

        redirect.get(RESTUtil.WebPath.HOME_URI, RESTUtil.WebPath.MATCH_QUERY_URI);

        get(RESTUtil.WebPath.MATCH_QUERY_URI, this::matchQuery);

        get(RESTUtil.WebPath.META_TYPE_INSTANCES_URI, this::buildMetaTypeInstancesObject);
    }


    private String buildMetaTypeInstancesObject(Request req, Response res){

        MindmapsTransactionImpl transaction = (MindmapsTransactionImpl) GraphFactory.getInstance().getGraph(graphName).newTransaction();

        JSONObject responseObj = new JSONObject();
        responseObj.put(RESTUtil.Response.ROLES_JSON_FIELD, new JSONArray(transaction.getMetaRoleType().instances().stream().map(x -> x.getId()).toArray()));
        responseObj.put(RESTUtil.Response.ENTITIES_JSON_FIELD, new JSONArray(transaction.getMetaEntityType().instances().stream().map(x -> x.getId()).toArray()));
        responseObj.put(RESTUtil.Response.RELATIONS_JSON_FIELD, new JSONArray(transaction.getMetaRelationType().instances().stream().map(x -> x.getId()).toArray()));
        responseObj.put(RESTUtil.Response.RESOURCES_JSON_FIELD, new JSONArray(transaction.getMetaResourceType().instances().stream().map(x -> x.getId()).toArray()));

        return responseObj.toString();
    }

    private String matchQuery(Request req, Response res) {

        QueryParser parser = QueryParser.create(GraphFactory.getInstance().getGraph(graphName).newTransaction());

        LOG.info("[ Received match query: \"" + req.queryParams(RESTUtil.Request.QUERY_FIELD) + "\"]");

        try {
            return parser.parseMatchQuery(req.queryParams(RESTUtil.Request.QUERY_FIELD))
                    .resultsString()
                    .map(x -> x.replaceAll("\u001B\\[\\d+[m]", ""))
                    .collect(Collectors.joining("\n"));
        } catch (Exception e) {
            res.status(400);
            return e.getMessage();
        }
    }


}
