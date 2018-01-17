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
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.engine.postprocessing;

import ai.grakn.GraknConfigKey;
import ai.grakn.Keyspace;
import ai.grakn.engine.tasks.BackgroundTask;
import ai.grakn.engine.tasks.manager.TaskConfiguration;
import ai.grakn.kb.log.CommitLog;
import ai.grakn.util.Schema;
import com.codahale.metrics.Timer.Context;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * <p>
 *     Task that control when postprocessing starts.
 * </p>
 *
 * <p>
 *     This task begins only if enough time has passed (configurable) since the last time a job was added.
 * </p>
 *
 * @author alexandraorth, fppt
 */
public class PostProcessingTask extends BackgroundTask {
    private static ObjectMapper mapper = new ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(PostProcessingTask.class);
    private static final String JOB_FINISHED = "Post processing Job [{}] completed for indeces and ids: [{}]";

    /**
     * Apply {@link ai.grakn.concept.Attribute} post processing jobs the concept ids in the provided configuration
     *
     * @return True if successful.
     */
    @Override
    public boolean start() {
        try (Context context = metricRegistry()
                .timer(name(PostProcessingTask.class, "execution")).time()) {
            CommitLog commitLog = getPostProcessingCommitLog(configuration());

            commitLog.attributes().forEach((conceptIndex, conceptIds) -> {
                Context contextSingle = metricRegistry()
                        .timer(name(PostProcessingTask.class, "execution-single")).time();
                try {
                    Keyspace keyspace = commitLog.keyspace();
                    int maxRetry = engineConfiguration().getProperty(GraknConfigKey.LOADER_REPEAT_COMMITS);

                    GraknTxMutators.runMutationWithRetry(factory(), keyspace, maxRetry,
                            (graph) -> postProcessor().mergeDuplicateConcepts(graph, conceptIndex, conceptIds));
                } finally {
                    contextSingle.stop();
                }
            });

            LOG.debug(JOB_FINISHED, Schema.BaseType.ATTRIBUTE.name(), commitLog.attributes());

            return true;
        }
    }

    /**
     * Extract a map of concept indices to concept ids from the provided configuration
     *
     * @param configuration Configuration from which to extract the {@link CommitLog}.
     * @return Map of concept indices to ids that has been extracted from the provided configuration.
     */
    private static CommitLog getPostProcessingCommitLog(TaskConfiguration configuration) {
        try {
            return mapper.readValue(configuration.configuration(), CommitLog.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}