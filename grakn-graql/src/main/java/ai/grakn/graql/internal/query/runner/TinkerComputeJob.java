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

package ai.grakn.graql.internal.query.runner;

import ai.grakn.ComputeJob;
import ai.grakn.GraknComputer;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.concept.LabelId;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.analytics.ComputeQuery;
import ai.grakn.graql.analytics.ConnectedComponentQuery;
import ai.grakn.graql.analytics.CorenessQuery;
import ai.grakn.graql.analytics.DegreeQuery;
import ai.grakn.graql.analytics.KCoreQuery;
import ai.grakn.graql.internal.analytics.ClusterMemberMapReduce;
import ai.grakn.graql.internal.analytics.ClusterSizeMapReduce;
import ai.grakn.graql.internal.analytics.ConnectedComponentVertexProgram;
import ai.grakn.graql.internal.analytics.ConnectedComponentsVertexProgram;
import ai.grakn.graql.internal.analytics.CorenessVertexProgram;
import ai.grakn.graql.internal.analytics.DegreeDistributionMapReduce;
import ai.grakn.graql.internal.analytics.DegreeVertexProgram;
import ai.grakn.graql.internal.analytics.GraknMapReduce;
import ai.grakn.graql.internal.analytics.GraknVertexProgram;
import ai.grakn.graql.internal.analytics.KCoreVertexProgram;
import ai.grakn.graql.internal.analytics.NoResultException;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A compute query job executed against a {@link GraknComputer}.
 *
 * @author Haikal Pribadi
 * @author Ganeshwara Herawan Hananda
 */
class TinkerComputeJob<T> implements ComputeJob<T> {

    private final ComputeQuery<?> query;

    private static final Logger LOG = LoggerFactory.getLogger(TinkerComputeJob.class);
    private final EmbeddedGraknTx<?> tx;

    public TinkerComputeJob(EmbeddedGraknTx<?> tx, ComputeQuery<?> query) {
        this.tx = tx;
        this.query = query;
    }

    @Override
    public void kill() { //todo: to be removed;
        tx.session().getGraphComputer().killJobs();
    }

    @Override
    public T get() {
        if (query instanceof DegreeQuery) return (T) runComputeDegree();
        if (query instanceof CorenessQuery) return (T) runComputeCoreness();
        if (query instanceof ConnectedComponentQuery) return (T) runComputeConnectedComponent();
        if (query instanceof KCoreQuery) return (T) runComputeKCore();

        throw GraqlQueryException.invalidComputeMethod();
    }

    private Map<Long, Set<String>> runComputeDegree() {
        TinkerComputeQuery<DegreeQuery> tinkerComputeQuery = new TinkerComputeQuery(tx, query);
        Set<Label> ofLabels;

        // Check if ofType is valid before returning emptyMap
        if (((DegreeQuery) query).ofTypes().isEmpty()) {
            ofLabels = tinkerComputeQuery.inTypeLabels();
        } else {
            ofLabels = ((DegreeQuery) query).ofTypes().stream()
                    .flatMap(typeLabel -> {
                        Type type = tx.getSchemaConcept(typeLabel);
                        if (type == null) throw GraqlQueryException.labelNotFound(typeLabel);
                        return type.subs();
                    })
                    .map(SchemaConcept::getLabel)
                    .collect(Collectors.toSet());
        }

        Set<Label> subLabels = Sets.union(tinkerComputeQuery.inTypeLabels(), ofLabels);

        if (!tinkerComputeQuery.inTypesHaveInstances()) {
            return Collections.emptyMap();
        }

        Set<LabelId> subLabelIds = convertLabelsToIds(subLabels);
        Set<LabelId> ofLabelIds = convertLabelsToIds(ofLabels);

        ComputerResult result = tinkerComputeQuery.compute(
                new DegreeVertexProgram(ofLabelIds),
                new DegreeDistributionMapReduce(ofLabelIds, DegreeVertexProgram.DEGREE),
                subLabelIds);

        return result.memory().get(DegreeDistributionMapReduce.class.getName());
    }

    private Map<Long, Set<String>> runComputeCoreness() {
        TinkerComputeQuery<CorenessQuery> tinkerComputeQuery = new TinkerComputeQuery(tx, query);
        long k = ((CorenessQuery) query).minK();

        if (k < 2L) throw GraqlQueryException.kValueSmallerThanTwo();

        Set<Label> ofLabels;

        // Check if ofType is valid before returning emptyMap
        if (((CorenessQuery) query).ofTypes().isEmpty()) {
            ofLabels = tinkerComputeQuery.inTypeLabels();
        } else {
            ofLabels = ((CorenessQuery) query).ofTypes().stream()
                    .flatMap(typeLabel -> {
                        Type type = tx.getSchemaConcept(typeLabel);
                        if (type == null) throw GraqlQueryException.labelNotFound(typeLabel);
                        if (type.isRelationshipType()) throw GraqlQueryException.kCoreOnRelationshipType(typeLabel);
                        return type.subs();
                    })
                    .map(SchemaConcept::getLabel)
                    .collect(Collectors.toSet());
        }

        Set<Label> subLabels = Sets.union(tinkerComputeQuery.inTypeLabels(), ofLabels);

        if (!tinkerComputeQuery.inTypesHaveInstances()) {
            return Collections.emptyMap();
        }

        ComputerResult result;
        Set<LabelId> subLabelIds = convertLabelsToIds(subLabels);
        Set<LabelId> ofLabelIds = convertLabelsToIds(ofLabels);

        try {
            result = tinkerComputeQuery.compute(
                    new CorenessVertexProgram(k),
                    new DegreeDistributionMapReduce(ofLabelIds, CorenessVertexProgram.CORENESS),
                    subLabelIds);
        } catch (NoResultException e) {
            return Collections.emptyMap();
        }

        return result.memory().get(DegreeDistributionMapReduce.class.getName());
    }

    private <T> T runComputeConnectedComponent() {
        ConnectedComponentQuery<T> ccQuery = (ConnectedComponentQuery) query;

        TinkerComputeQuery<ConnectedComponentQuery<T>> tinkerComputeQuery = new TinkerComputeQuery(tx, query);
        if (!tinkerComputeQuery.inTypesHaveInstances()) {
            LOG.info("Selected types don't have instances");
            return (T) Collections.emptyMap();
        }

        Set<LabelId> subLabelIds = convertLabelsToIds(tinkerComputeQuery.inTypeLabels());


        GraknVertexProgram<?> vertexProgram;
        if (ccQuery.start().isPresent()) {
            ConceptId conceptId = ccQuery.start().get();
            if (!tinkerComputeQuery.inTypesContainConcepts(conceptId)) {
                throw GraqlQueryException.instanceDoesNotExist();
            }
            vertexProgram = new ConnectedComponentVertexProgram(conceptId);
        } else {
            vertexProgram = new ConnectedComponentsVertexProgram();
        }

        GraknMapReduce<?> mapReduce;

        if (ccQuery.size().isPresent()) {
            if (ccQuery.isMembersSet()) {
                mapReduce = new ClusterMemberMapReduce(ConnectedComponentsVertexProgram.CLUSTER_LABEL, ccQuery.size().get());
            } else {
                mapReduce = new ClusterSizeMapReduce(ConnectedComponentsVertexProgram.CLUSTER_LABEL, ccQuery.size().get());
            }
        } else {
            if (ccQuery.isMembersSet()) {
                mapReduce = new ClusterMemberMapReduce(ConnectedComponentsVertexProgram.CLUSTER_LABEL);
            } else {
                mapReduce = new ClusterSizeMapReduce(ConnectedComponentsVertexProgram.CLUSTER_LABEL);
            }
        }

        Memory memory = tinkerComputeQuery.compute(vertexProgram, mapReduce, subLabelIds).memory();
        return memory.get(mapReduce.getClass().getName());
    }

    private Map<String, Set<String>> runComputeKCore() {
        TinkerComputeQuery<KCoreQuery> tinkerComputeQuery = new TinkerComputeQuery(tx, query);
        long k = ((KCoreQuery) query).k();

        if (k < 2L) throw GraqlQueryException.kValueSmallerThanTwo();

        if (!tinkerComputeQuery.inTypesHaveInstances()) {
            return Collections.emptyMap();
        }

        ComputerResult result;
        Set<LabelId> subLabelIds = convertLabelsToIds(tinkerComputeQuery.inTypeLabels());
        try {
            result = tinkerComputeQuery.compute(
                    new KCoreVertexProgram(k),
                    new ClusterMemberMapReduce(KCoreVertexProgram.K_CORE_LABEL),
                    subLabelIds);
        } catch (NoResultException e) {
            return Collections.emptyMap();
        }

        return result.memory().get(ClusterMemberMapReduce.class.getName());
    }


    private Set<LabelId> convertLabelsToIds(Set<Label> labelSet) {
        return labelSet.stream()
                .map(tx::convertToId)
                .filter(LabelId::isValid)
                .collect(Collectors.toSet());
    }

}
