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

import io.mindmaps.core.implementation.DataType;
import io.mindmaps.postprocessing.Cache;
import io.mindmaps.util.ConfigProperties;
import io.mindmaps.util.ErrorMessage;
import io.mindmaps.util.RESTUtil;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import static spark.Spark.post;

/**
 * A controller which core submits commit logs to so we can post-process jobs for cleanup.
 */
public class CommitLogController {
    private final Cache cache;
    private final Logger LOG = LoggerFactory.getLogger(CommitLogController.class);

    public CommitLogController(){
        cache = Cache.getInstance();
        post(RESTUtil.WebPath.COMMIT_LOG_URI, this::submitConcepts);
    }

    /**
     *
     * @param req The request which contains the graph to be post processed
     * @param res The current response code
     * @return The result of adding something for post processing
     */
    private String submitConcepts(Request req, Response res) {
        String graphName = req.queryParams(RESTUtil.Request.GRAPH_NAME_PARAM);

        if(graphName == null){
            graphName = ConfigProperties.getInstance().getProperty(ConfigProperties.DEFAULT_GRAPH_NAME_PROPERTY);
        }
        LOG.info("Commit log received for graph [" + graphName + "]");

        JSONArray jsonArray = (JSONArray) new JSONObject(req.body()).get("concepts");

        for (Object object : jsonArray) {
            JSONObject jsonObject = (JSONObject) object;
            String conceptId = jsonObject.getString("id");
            DataType.BaseType type = DataType.BaseType.valueOf(jsonObject.getString("type"));

            switch (type){
                case RELATION:
                    cache.addJobRelation(graphName, conceptId);
                    break;
                case CASTING:
                    cache.addJobCasting(graphName, conceptId);
                    break;
                default:
                    LOG.warn(ErrorMessage.CONCEPT_POSTPROCESSING.getMessage(conceptId, type.name()));
            }
        }

        long numJobs =  cache.getCastingJobs().get(graphName).size() + cache.getRelationJobs().get(graphName).size();
        return "Graph [" + graphName + "] now has [" + numJobs + "] post processing jobs";
    }
}
