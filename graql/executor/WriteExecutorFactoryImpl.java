/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package grakn.core.graql.executor;

import com.google.common.collect.ImmutableSet;
import grakn.core.concept.answer.AnswerUtil;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.answer.Void;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.graql.executor.WriteExecutor;
import grakn.core.kb.graql.executor.WriteExecutorFactory;
import grakn.core.kb.graql.executor.property.PropertyExecutor;
import grakn.core.kb.graql.executor.property.PropertyExecutorFactory;
import grakn.core.kb.server.Transaction;
import grakn.core.kb.server.exception.GraknServerException;
import grakn.core.kb.server.exception.GraqlSemanticException;
import graql.lang.property.VarProperty;
import graql.lang.query.GraqlDefine;
import graql.lang.query.GraqlDelete;
import graql.lang.query.GraqlUndefine;
import graql.lang.statement.Statement;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class WriteExecutorFactoryImpl implements WriteExecutorFactory {

    private Transaction transaction;
    private ConceptManager conceptManager;
    private PropertyExecutorFactory propertyExecutorFactory;

    WriteExecutorFactoryImpl(Transaction transaction, ConceptManager conceptManager, PropertyExecutorFactory propertyExecutorFactory) {
        this.transaction = transaction;
        this.conceptManager = conceptManager;
        this.propertyExecutorFactory = propertyExecutorFactory;
    }

    @Override
    public WriteExecutor define(GraqlDefine query) {
        ImmutableSet.Builder<PropertyExecutor.Writer> executors = ImmutableSet.builder();
        List<Statement> statements = query.statements().stream()
                .flatMap(statement -> statement.innerStatements().stream())
                .collect(Collectors.toList());

        for (Statement statement : statements) {
            for (VarProperty property : statement.properties()) {
                executors.addAll(propertyExecutorFactory.definable(statement.var(), property).defineExecutors());
            }
        }

        return WriteExecutorImpl.create(transaction, conceptManager, executors.build());
    }

    @Override
    public WriteExecutor undefine(GraqlUndefine query) {
        ImmutableSet.Builder<PropertyExecutor.Writer> executors = ImmutableSet.builder();
        List<Statement> statements = query.statements().stream()
                .flatMap(statement -> statement.innerStatements().stream())
                .collect(Collectors.toList());

        for (Statement statement : statements) {
            for (VarProperty property : statement.properties()) {
                executors.addAll(propertyExecutorFactory.definable(statement.var(), property).undefineExecutors());
            }
        }
        return WriteExecutorImpl.create(transaction, conceptManager, executors.build());
    }

    @Override
    public WriteExecutor delete(GraqlDelete query) {
        Stream<ConceptMap> answers = transaction.stream(query.match(), infer)
                .map(result -> result.project(query.vars()))
                .distinct();

        answers = AnswerUtil.filter(query, answers);

        // TODO: We should not need to collect toSet, once we fix ConceptId.id() to not use cache.
        List<Concept> conceptsToDelete = answers
                .flatMap(answer -> answer.concepts().stream())
                .distinct()
                // delete relations first: if the RPs are deleted, the relation is removed, so null by the time we try to delete it
                // this minimises number of `concept was already removed` exceptions
                .sorted(Comparator.comparing(concept -> !concept.isRelation()))
                .collect(Collectors.toList());


        conceptsToDelete.forEach(concept -> {
            // a concept is either a schema concept or a thing
            if (concept.isSchemaConcept()) {
                throw GraqlSemanticException.deleteSchemaConcept(concept.asSchemaConcept());
            } else if (concept.isThing()) {
                try {
                    // a concept may have been cleaned up already
                    // for instance if role players of an implicit attribute relation are deleted, the janus edge disappears
                    concept.delete();
                } catch (IllegalStateException janusVertexDeleted) {
                    if (janusVertexDeleted.getMessage().contains("was removed")) {
                        // Tinkerpop throws this exception if we try to operate on a vertex that was already deleted
                        // With the ordering of deletes, this edge case should only be hit when relations play roles in relations
                        LOG.debug("Trying to deleted concept that was already removed", janusVertexDeleted);
                    } else {
                        throw janusVertexDeleted;
                    }
                }
            } else {
                throw GraknServerException.create("Unhandled concept type isn't a schema concept or a thing");
            }
        });

        // TODO: return deleted Concepts instead of ConceptIds
        return new Void("Delete successful.");
    }

}
