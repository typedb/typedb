/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.query;

import com.vaticle.factory.tracing.client.FactoryTracingThreadStatic;
import com.vaticle.typedb.common.collection.Either;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.parameters.Context;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.pattern.variable.ThingVariable;
import com.vaticle.typedb.core.pattern.variable.VariableRegistry;
import com.vaticle.typedb.core.reasoner.Reasoner;
import com.vaticle.typeql.lang.pattern.variable.UnboundVariable;
import com.vaticle.typeql.lang.query.TypeQLUpdate;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static com.vaticle.factory.tracing.client.FactoryTracingThreadStatic.traceOnThread;
import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.ILLEGAL_ANONYMOUS_VARIABLE_IN_DELETE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.ILLEGAL_TYPE_VARIABLE_IN_DELETE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.ILLEGAL_TYPE_VARIABLE_IN_INSERT;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.common.parameters.Arguments.Query.Producer.EXHAUSTIVE;
import static com.vaticle.typedb.core.concurrent.executor.Executors.PARALLELISATION_FACTOR;
import static com.vaticle.typedb.core.concurrent.executor.Executors.async1;
import static com.vaticle.typedb.core.concurrent.producer.Producers.async;
import static com.vaticle.typedb.core.concurrent.producer.Producers.produce;
import static com.vaticle.typedb.core.query.QueryManager.PARALLELISATION_SPLIT_MIN;

public class Updater {

    private static final String TRACE_PREFIX = "updater.";
    private final Matcher matcher;
    private final ConceptManager conceptMgr;
    private final Set<ThingVariable> deleteVariables;
    private final Set<ThingVariable> insertVariables;
    private final Context.Query context;

    public Updater(Matcher matcher, ConceptManager conceptMgr, Set<ThingVariable> deleteVariables,
                   Set<ThingVariable> insertVariables, Context.Query context) {
        this.matcher = matcher;
        this.conceptMgr = conceptMgr;
        this.deleteVariables = deleteVariables;
        this.insertVariables = insertVariables;
        this.context = context;
    }

    public static Updater create(Reasoner reasoner, ConceptManager conceptMgr, TypeQLUpdate query, Context.Query context) {
        try (FactoryTracingThreadStatic.ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "create")) {
            VariableRegistry deleteRegistry = VariableRegistry.createFromThings(query.deleteVariables(), false);
            deleteRegistry.variables().forEach(Deleter::validate);

            VariableRegistry insertRegistry = VariableRegistry.createFromThings(query.insertVariables());
            insertRegistry.variables().forEach(Inserter::validate);

            assert query.match().namedVariablesUnbound().containsAll(query.namedDeleteVariablesUnbound());
            HashSet<UnboundVariable> filter = new HashSet<>(query.namedDeleteVariablesUnbound());
            filter.addAll(query.namedInsertVariablesUnbound());
            Matcher matcher = Matcher.create(reasoner, query.match().get(list(filter)));
            return new Updater(matcher, conceptMgr, deleteRegistry.things(), insertRegistry.things(), context);
        }
    }

    public FunctionalIterator<ConceptMap> execute() {
        try (FactoryTracingThreadStatic.ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "execute")) {
            return context.options().parallel() ? executeParallel() : executeSerial();
        }
    }

    private FunctionalIterator<ConceptMap> executeParallel() {
        List<List<ConceptMap>> lists = matcher.execute(context).toLists(PARALLELISATION_SPLIT_MIN, PARALLELISATION_FACTOR);
        assert !lists.isEmpty();
        List<ConceptMap> updates;
        Function<ConceptMap, ConceptMap> updateFn = (matched) -> {
            new Deleter.Operation(matched, deleteVariables).execute();
            return new Inserter.Operation(conceptMgr, matched, insertVariables).execute();
        };
        if (lists.size() == 1) updates = iterate(lists.get(0)).map(updateFn).toList();
        else updates = produce(async(
                iterate(lists).map(list -> iterate(list).map(updateFn)), PARALLELISATION_FACTOR
        ), Either.first(EXHAUSTIVE), async1()).toList();
        return iterate(updates);
    }

    private FunctionalIterator<ConceptMap> executeSerial() {
        List<ConceptMap> matches = matcher.execute(context).onError(conceptMgr::exception).toList();
        List<ConceptMap> answers = iterate(matches).map(matched -> {
            new Deleter.Operation(matched, deleteVariables).execute();
            return new Inserter.Operation(conceptMgr, matched, insertVariables).execute();
        }).toList();
        return iterate(answers);
    }
}
