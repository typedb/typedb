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

package ai.grakn.engine.controller;

import ai.grakn.engine.postprocessing.Cache;
import ai.grakn.engine.util.ConfigProperties;
import ai.grakn.exception.GraknEngineServerException;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.REST;
import ai.grakn.util.Schema;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import java.util.Collections;
import java.util.Set;

import static spark.Spark.delete;
import static spark.Spark.post;

/**
 * A controller which core submits commit logs to so we can post-process jobs for cleanup.
 */
public class CommitLogController {
    private final Cache cache = Cache.getInstance();
    private final Logger LOG = LoggerFactory.getLogger(CommitLogController.class);

    public CommitLogController(){
        post(REST.WebPath.COMMIT_LOG_URI, this::submitConcepts);
        delete(REST.WebPath.COMMIT_LOG_URI, this::deleteConcepts);
    }

    /**
     *
     * @param req The request which contains the graph to be post processed
     * @param res The current response code
     * @return The result of clearing the post processing for a single graph
     */
    private String deleteConcepts(Request req, Response res){
        String graphName = req.queryParams(REST.Request.KEYSPACE_PARAM);

        if(graphName == null){
            res.status(400);
           return ErrorMessage.NO_PARAMETER_PROVIDED.getMessage(REST.Request.KEYSPACE_PARAM, "delete");
        }

        cache.getCastingJobs(graphName).clear();
        cache.getResourceJobs(graphName).clear();

        return "The cache of Graph [" + graphName + "] has been cleared";
    }

    /**
     *
     * @param req The request which contains the graph to be post processed
     * @param res The current response code
     * @return The result of adding something for post processing
     */
    private String submitConcepts(Request req, Response res) {
        try {
            String graphName = req.queryParams(REST.Request.KEYSPACE_PARAM);

            if (graphName == null) {
                graphName = ConfigProperties.getInstance().getProperty(ConfigProperties.DEFAULT_KEYSPACE_PROPERTY);
            }
            LOG.info("Commit log received for graph [" + graphName + "]");

            JSONArray jsonArray = (JSONArray) new JSONObject(req.body()).get("concepts");

            for (Object object : jsonArray) {
                JSONObject jsonObject = (JSONObject) object;
                String conceptId = jsonObject.getString("id");
                Schema.BaseType type = Schema.BaseType.valueOf(jsonObject.getString("type"));

                switch (type) {
                    case CASTING:
                        cache.addJobCasting(graphName, Collections.singleton(conceptId));
                        break;
                    case RESOURCE:
                        cache.addJobResource(graphName, Collections.singleton(conceptId));
                    default:
                        LOG.warn(ErrorMessage.CONCEPT_POSTPROCESSING.getMessage(conceptId, type.name()));
                }
            }

            long numJobs = getJobCount(cache.getCastingJobs(graphName));
            numJobs += getJobCount(cache.getResourceJobs(graphName));

            return "Graph [" + graphName + "] now has [" + numJobs + "] post processing jobs";
        } catch(Exception e){
            throw new GraknEngineServerException(500,e);
        }
    }
    private long getJobCount(Set jobs){
        if(jobs != null)
            return jobs.size();
        return 0L;
    }
}
