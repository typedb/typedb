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

import ai.grakn.concept.ConceptId;
import ai.grakn.engine.util.ConfigProperties;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.factory.EngineGraknGraphFactory;
import ai.grakn.graph.EngineGraknGraph;
import ai.grakn.util.EngineCache;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

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

    public static void checkResources(String keyspace, String index, Set<ConceptId> conceptIds){
        runPostProcessingJob(ConceptFixer::runResourceFix, keyspace, index, conceptIds);
    }

    public static void checkCastings(String keyspace, String index, Set<ConceptId> conceptIds){
        runPostProcessingJob(ConceptFixer::runCastingFix, keyspace, index, conceptIds);
    }

    /**
     * Main method which attempts to run all post processing jobs.
     *
     * @param postProcessor The post processing job.
     *                      Either {@link ConceptFixer#runResourceFix(EngineGraknGraph, Set)} or
     *                      {@link ConceptFixer#runCastingFix(EngineGraknGraph, Set)}.
     *                      This then returns a function which will complete the job after going through validation
     * @param keyspace The keyspace to post process against.
     * @param conceptIndex The unique index of the concept which must exist at the end
     * @param conceptIds The conceptIds which effectively need to be merged.
     * @return true if the job was executed successfully, otherwise false.
     */
    private static boolean runPostProcessingJob(BiFunction<EngineGraknGraph, Set<ConceptId>, BiConsumer<EngineCache, String>> postProcessor,
                                                String keyspace, String conceptIndex, Set<ConceptId> conceptIds){
        String jobId = UUID.randomUUID().toString();
        boolean notDone = true;
        int retry = 0;

        while (notDone) {
            //Try to Fix the job
            try(EngineGraknGraph graph = EngineGraknGraphFactory.getInstance().getGraph(keyspace))  {

                //Perform the fix
                BiConsumer<EngineCache, String> jobFinaliser = postProcessor.apply(graph, conceptIds);

                //Check if the fix worked
                validateMerged(graph, conceptIndex, conceptIds).
                        ifPresent(message -> {
                            throw new RuntimeException(message);
                        });

                //Finally clear the cache
                jobFinaliser.accept(EngineCacheImpl.getInstance(), conceptIndex);

                return true; //If it can get here. All post processing has succeeded.
            } catch (Throwable t) { //These exceptions need to become more specialised
                LOG.warn(ErrorMessage.POSTPROCESSING_ERROR.getMessage(jobId, t.getMessage()), t);
            }

            //Fixing the job has failed after several attempts
            if (retry > MAX_RETRY) {
                notDone = false;
            }

            retry = performRetry(retry);
        }

        StringBuilder failingConcepts = new StringBuilder();
        conceptIds.stream().map(id -> failingConcepts.append(id.getValue()).append(","));
        LOG.error(ErrorMessage.UNABLE_TO_ANALYSE_CONCEPT.getMessage(failingConcepts, jobId));
        return false;
    }

    /**
     * Checks that post processing was done successfully by doing two things:
     *  1. That there is only 1 valid conceptID left
     *  2. That the concept Index does not return null
     * @param graph A grakn graph to run the checks against.
     * @param conceptIndex The concept index which MUST return a valid concept
     * @param conceptIds The concpet ids which should only return 1 valid concept
     * @return An error if one of the above rules are not satisfied.
     */
    private static Optional<String> validateMerged(EngineGraknGraph graph, String conceptIndex, Set<ConceptId> conceptIds){
        //Check number of valid concept Ids
        int numConceptFound = 0;
        for (ConceptId conceptId : conceptIds) {
            if (graph.getConcept(conceptId) != null) {
                numConceptFound++;
                if (numConceptFound > 1) {
                    StringBuilder conceptIdValues = new StringBuilder();
                    conceptIds.stream().map(c -> conceptIdValues.append(c.getValue()).append(","));
                    return Optional.of("Not all concept were merged. The set of concept IDs [" + conceptIdValues + "] matched more than one concept");
                }
            }
        }

        //Check index
        if(graph.getConcept(Schema.ConceptProperty.INDEX, conceptIndex) == null){
            return Optional.of("The concept index [" + conceptIndex + "] did not return any concept");
        }

        return Optional.empty();
    }

    private static BiConsumer<EngineCache, String> runResourceFix(EngineGraknGraph graph, Set<ConceptId> conceptIds) {
        if(graph.fixDuplicateResources(conceptIds)){
            try {
                graph.commit(null); //TODO: Fix null passing here. Null is passed to avoid sending commit logs again
            } catch (GraknValidationException e) {
                throw new RuntimeException(e);
            }
        }

        return (cache, index) -> {conceptIds.
                forEach(conceptId -> cache.deleteJobResource(graph.getKeyspace(), index, conceptId));};
    }

    private static BiConsumer<EngineCache, String> runCastingFix(EngineGraknGraph graph, Set<ConceptId> conceptIds) {
        if(graph.fixDuplicateCastings(conceptIds)){
            try {
                graph.commit(null); //TODO: Fix null passing here. Null is passed to avoid sending commit logs again
            } catch (GraknValidationException e) {
                throw new RuntimeException(e);
            }
        }

        return (cache, index) -> {conceptIds.
                forEach(conceptId -> cache.deleteJobCasting(graph.getKeyspace(), index, conceptId));};
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
