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

package ai.grakn.engine.task.postprocessing;

import ai.grakn.GraknConfigKey;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.ConceptId;
import ai.grakn.engine.GraknConfig;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.engine.task.BackgroundTask;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
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
        Set<Keyspace> kespaces = factory.systemKeyspace().keyspaces();
        kespaces.forEach(keyspace -> {
            String index;
            int limit = 0;
            do{
                 index = indexPostProcessor.popIndex(keyspace);
                 final String i = index;
                 if(index != null) threadPool.schedule(() -> processIndex(keyspace, i), postprocessingDelay, TimeUnit.SECONDS);
                 limit++;
            } while(index != null && limit < postProcessingMaxJobs);
        });
    }

    /**
     * Process the provided index belonging to the provided {@link Keyspace}.
     * If post processing fails the index and ids relating to that index are restored back in the cache of {@link RedisIndexStorage}
     *
     * @param keyspace The {@link Keyspace} requiring post processing for a specific index
     * @param index the index to be post processed
     */
    public void processIndex(Keyspace keyspace, String index){
        Set<ConceptId> ids = indexPostProcessor.popIds(keyspace, index);

        //No need to post process if another engine has beaten you to doing it
        if(ids.isEmpty()) return;

        try(EmbeddedGraknTx<?> tx = factory.tx(keyspace, GraknTxType.WRITE)){
            indexPostProcessor.mergeDuplicateConcepts(tx, index, ids);
            tx.commit();
        } catch (RuntimeException e){
            String stringIds = ids.stream().map(ConceptId::getValue).collect(Collectors.joining(","));
            LOG.error(String.format("Error during post processing index {%s} with ids {%s}", index, stringIds), e);
        }
    }

    @Override
    public void close(){
        threadPool.shutdown();
    }

}
