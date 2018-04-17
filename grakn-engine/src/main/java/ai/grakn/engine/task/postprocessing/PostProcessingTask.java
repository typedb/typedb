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

package ai.grakn.engine.task.postprocessing;

import ai.grakn.GraknConfigKey;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.ConceptId;
import ai.grakn.engine.GraknConfig;
import ai.grakn.engine.GraknKeyspaceStore;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.engine.task.BackgroundTask;
import ai.grakn.engine.task.postprocessing.redisstorage.RedisIndexStorage;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * <p>
 *     Class which facilitates running {@link PostProcessor} jobs.
 * </p>
 *
 * @author Filipe Peliz Pinto Teixeira
 */
public class PostProcessingTask implements BackgroundTask{
    private static final Logger LOG = LoggerFactory.getLogger(PostProcessingTask.class);
    private final EngineGraknTxFactory factory;
    private final IndexPostProcessor indexPostProcessor;
    private final ScheduledExecutorService threadPool;
    private final int postProcessingMaxJobs;
    private final int postprocessingDelay;

    public PostProcessingTask(EngineGraknTxFactory factory,  IndexPostProcessor indexPostProcessor, GraknConfig config){
        this.factory = factory;
        this.indexPostProcessor = indexPostProcessor;
        this.postProcessingMaxJobs = config.getProperty(GraknConfigKey.POST_PROCESSOR_POOL_SIZE);
        this.threadPool = Executors.newScheduledThreadPool(postProcessingMaxJobs);
        this.postprocessingDelay = config.getProperty(GraknConfigKey.POST_PROCESSOR_DELAY);
    }

    @Override
    public void run() {
        UUID executionId = UUID.randomUUID();
        LOG.info("starting post-processing task with ID '" + executionId + "' ... ");
        GraknKeyspaceStore keyspaceStore = factory.keyspaceStore();
        if (keyspaceStore != null) {
            Set<Keyspace> keyspaces = keyspaceStore.keyspaces();
            LOG.info("post-processing '" + executionId + "': attempting to process the following keyspaces: [" +
                    keyspaces.stream().map(Keyspace::getValue).collect(Collectors.joining(", ")) + "]");
            keyspaces.forEach(keyspace -> runPostProcessing(executionId, keyspace));
            LOG.info("post-processing task with ID " + executionId + "finished.");
        } else {
            LOG.info("post-processing " + executionId + ": waiting for system keyspace to be ready.");
        }
    }

    private void runPostProcessing(UUID executionId, Keyspace keyspace) {
        String index;
        int limit = 0;
        do {
            index = indexPostProcessor.popIndex(keyspace);
            LOG.info("post-processing '" + executionId + "': working on keyspace '" + keyspace.getValue() +
                    "'. The index to be post-processed is '" + index + "'");
            final String i = index;
            if (index != null) {
                threadPool.schedule(() -> processIndex(keyspace, i, executionId), postprocessingDelay, TimeUnit.SECONDS);
            }
            limit++;
        } while (index != null && limit < postProcessingMaxJobs);
    }

    /**
     * Process the provided index belonging to the provided {@link Keyspace}.
     * If post processing fails the index and ids relating to that index are restored back in the cache of {@link RedisIndexStorage}
     *
     * @param keyspace The {@link Keyspace} requiring post processing for a specific index
     * @param index the index to be post processed
     * @param executionId execution id of the post-processing.
     */
    private void processIndex(Keyspace keyspace, String index, UUID executionId){
        Set<ConceptId> ids = indexPostProcessor.popIds(keyspace, index);
        //No need to post process if another engine has beaten you to doing it
        if(ids.isEmpty()) {
            LOG.info("post-processing '" + executionId + "': there " + ids.size() + " concept ids to post-process.");
            return;
        }

        LOG.info("post-processing '" + executionId + "': processing " + ids.size() + " concept ids...");

        try(EmbeddedGraknTx<?> tx = factory.tx(keyspace, GraknTxType.WRITE)){
            indexPostProcessor.mergeDuplicateConcepts(tx, index, ids);
            tx.commit();
        } catch (RuntimeException e){
            String stringIds = ids.stream().map(ConceptId::getValue).collect(Collectors.joining(","));
            LOG.error(String.format("post-processing '" + executionId + "': Error during post processing index {%s} with ids {%s}", index, stringIds), e);
        }
    }

    @Override
    public void close(){
        LOG.info("post-processing is shutting down.");
        threadPool.shutdown();
    }

}
