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

package ai.grakn.engine.controller;

import ai.grakn.engine.task.postprocessing.PostProcessor;
import ai.grakn.kb.log.CommitLog;
import ai.grakn.util.REST;
import com.fasterxml.jackson.databind.ObjectMapper;
import spark.Request;
import spark.Service;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * A controller which core submits commit logs to so we can post-process jobs for cleanup.
 *
 * @author Filipe Peliz Pinto Teixeira
 */
public class CommitLogController implements HttpController {
    private static final ObjectMapper mapper = new ObjectMapper();
    private final PostProcessor postProcessor;

    public CommitLogController(PostProcessor postProcessor){
        this.postProcessor = postProcessor;

    }

    @POST
    @Path("/kb/{keyspace}/commit_log")
    private String submitConcepts(Request req) throws IOException {
        CommitLog commitLog = mapper.readValue(req.body(), CommitLog.class);
        CompletableFuture.allOf(CompletableFuture.runAsync(() -> postProcessor.submit(commitLog))).join();
        return "";
    }

    @Override
    public void start(Service spark) {
        spark.post(REST.WebPath.COMMIT_LOG_URI, (req, res) -> submitConcepts(req));
    }
}
