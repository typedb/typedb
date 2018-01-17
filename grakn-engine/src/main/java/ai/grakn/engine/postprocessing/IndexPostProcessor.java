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

import ai.grakn.GraknTx;
import ai.grakn.concept.ConceptId;
import ai.grakn.engine.lock.LockProvider;
import ai.grakn.util.Schema;
import com.google.common.base.Preconditions;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.Lock;

/**
 * <p>
 *     Class responsible for merging duplicate {@link ai.grakn.concept.Attribute}s which may result from creating
 *     {@link ai.grakn.concept.Attribute}s in a concurrent environment
 * </p>
 *
 * @author Filipe Peliz Pinto Teixeira
 */
public class IndexPostProcessor {
    private final LockProvider lockProvider;

    @Deprecated
    private static final String LOCK_KEY = "/post-processing-lock";

    IndexPostProcessor(LockProvider lockProvider) {
        this.lockProvider = lockProvider;
    }

    public static IndexPostProcessor create(LockProvider lockProvider) {
        return new IndexPostProcessor(lockProvider);
    }

    public void updateIndices(){
        
    }

    /**
     * Merges duplicate {@link ai.grakn.concept.Concept}s based on the unique index provided plus the {@link ConceptId}s
     * of the suspected duplicates
     *
     * @param tx The {@link GraknTx} responsible for performing the merge
     * @param conceptIndex The unique {@link ai.grakn.concept.Concept} index which is supposed to exist only once
     *                     across the entire DB.
     * @param conceptIds The {@link ConceptId}s of the suspected duplicates
     */
    public void mergeDuplicateConcepts(GraknTx tx, String conceptIndex, Set<ConceptId> conceptIds){
        Preconditions.checkNotNull(lockProvider, "Lock provider was null, possible race condition in initialisation");
        if(tx.admin().duplicateResourcesExist(conceptIndex, conceptIds)){

            // Acquire a lock when you post process on an index to prevent race conditions
            // Lock is acquired after checking for duplicates to reduce runtime
            Lock indexLock = lockProvider.getLock(LOCK_KEY + "/" + conceptIndex);
            indexLock.lock();

            try {
                // execute the provided post processing method
                boolean commitNeeded = tx.admin().fixDuplicateResources(conceptIndex, conceptIds);

                // ensure post processing was correctly executed
                if(commitNeeded) {
                    validateMerged(tx, conceptIndex, conceptIds).
                            ifPresent(message -> {
                                throw new RuntimeException(message);
                            });

                    // persist merged concepts
                    tx.admin().commitSubmitNoLogs();
                }
            } finally {
                indexLock.unlock();
            }
        }
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
    private Optional<String> validateMerged(GraknTx graph, String conceptIndex, Set<ConceptId> conceptIds){
        //Check number of valid concept Ids
        int numConceptFound = 0;
        for (ConceptId conceptId : conceptIds) {
            if (graph.getConcept(conceptId) != null) {
                numConceptFound++;
                if (numConceptFound > 1) {
                    StringBuilder conceptIdValues = new StringBuilder();
                    for (ConceptId id : conceptIds) {
                        conceptIdValues.append(id.getValue()).append(",");
                    }
                    return Optional.of("Not all concept were merged. The set of concepts [" + conceptIds.size() + "] with IDs [" + conceptIdValues.toString() + "] matched more than one concept");
                }
            }
        }

        //Check index
        if(graph.admin().getConcept(Schema.VertexProperty.INDEX, conceptIndex) == null){
            return Optional.of("The concept index [" + conceptIndex + "] did not return any concept");
        }

        return Optional.empty();
    }
}
