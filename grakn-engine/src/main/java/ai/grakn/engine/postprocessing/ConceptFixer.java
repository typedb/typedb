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

package ai.grakn.engine.postprocessing;

import ai.grakn.engine.util.ConfigProperties;
import ai.grakn.factory.EngineGraknGraphFactory;
import ai.grakn.graph.internal.AbstractGraknGraph;
import ai.grakn.util.ErrorMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * <p>
 *     Post processing concept fixer
 * </p>
 *
 * <p>
 *     Executes the post processing protocols. At the moment this includes merging duplicate castings and duplicate
 *     resources.
 * </p>
 *
 * @author fppt
 */
class ConceptFixer {
    private static final Logger LOG = LoggerFactory.getLogger(ConfigProperties.LOG_NAME_POSTPROCESSING_DEFAULT);
    private static final int MAX_RETRY = 10;

    public static void checkCasting(Cache cache, String keyspace, String castingId){
        boolean notDone = true;
        int retry = 0;
        while (notDone) {
            try (AbstractGraknGraph graph = (AbstractGraknGraph) EngineGraknGraphFactory.getInstance().getGraph(keyspace)) {
                if (graph.fixDuplicateCasting(castingId)) {
                    graph.commit(false);
                }
                cache.deleteJobCasting(graph.getKeyspace(), castingId);
                notDone = false;
            } catch (Exception e) {
                LOG.warn(ErrorMessage.POSTPROCESSING_ERROR.getMessage("casting", e.getMessage()), e);
                if (retry++ > MAX_RETRY) {
                    LOG.error(ErrorMessage.UNABLE_TO_ANALYSE_CONCEPT.getMessage(castingId, e.getMessage()), e);
                    notDone = false;
                } else {
                    performRetry(retry);
                }
            }
        }
    }

    public static void checkResources(Cache cache, String keyspace, Set<String> resourceIds){
        boolean notDone = true;
        int retry = 0;

        while (notDone) {
            try(AbstractGraknGraph graph = (AbstractGraknGraph) EngineGraknGraphFactory.getInstance().getGraph(keyspace))  {
                if (graph.fixDuplicateResources(resourceIds)) {
                    graph.commit(false);
                }
                resourceIds.forEach(resourceId -> cache.deleteJobResource(graph.getKeyspace(), resourceId));
                notDone = false;
            } catch (Exception e) {
                LOG.warn(ErrorMessage.POSTPROCESSING_ERROR.getMessage("resource", e.getMessage()), e);
                if (retry++ > MAX_RETRY) {
                    StringBuilder message = new StringBuilder();
                    for (String resourceId : resourceIds) {
                        message.append(resourceId);
                    }
                    LOG.error(ErrorMessage.UNABLE_TO_ANALYSE_CONCEPT.getMessage(message, e.getMessage()), e);
                    notDone = false;
                } else {
                    performRetry(retry);
                }
            }
        }
    }

    private static int performRetry(int retry){
        retry ++;
        double seed = 1.0 + (Math.random() * 5.0);
        double waitTime = (retry * 2.0)  + seed;
        LOG.debug(ErrorMessage.BACK_OFF_RETRY.getMessage(waitTime));

        try {
            Thread.sleep((long) Math.ceil(waitTime * 1000));
        } catch (InterruptedException e1) {
            LOG.error("Exception",e1);
        }

        return retry;
    }
}
