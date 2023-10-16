/*
 * Copyright (C) 2022 Vaticle
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

import com.vaticle.typedb.common.collection.Either;
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

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.common.parameters.Arguments.Query.Producer.EXHAUSTIVE;
import static com.vaticle.typedb.core.concurrent.executor.Executors.PARALLELISATION_FACTOR;
import static com.vaticle.typedb.core.concurrent.executor.Executors.async1;
import static com.vaticle.typedb.core.concurrent.producer.Producers.async;
import static com.vaticle.typedb.core.concurrent.producer.Producers.produce;
import static com.vaticle.typedb.core.query.QueryManager.PARALLELISATION_SPLIT_MIN;

public class Updater {

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
        VariableRegistry deleteRegistry = VariableRegistry.createFromThings(query.deleteVariables(), false);
        deleteRegistry.variables().forEach(Deleter::validate);

        assert query.match().namedVariablesUnbound().containsAll(query.namedDeleteVariablesUnbound());
        Set<UnboundVariable> filter = new HashSet<>(query.match().namedVariablesUnbound());
        filter.retainAll(query.namedInsertVariablesUnbound());
        filter.addAll(query.namedDeleteVariablesUnbound());
        Matcher matcher = Matcher.create(reasoner, conceptMgr, query.match().get(list(filter)));

        VariableRegistry insertRegistry = VariableRegistry.createFromThings(query.insertVariables());
        insertRegistry.variables().forEach(var -> Inserter.validate(var, matcher));
        return new Updater(matcher, conceptMgr, deleteRegistry.things(), insertRegistry.things(), context);
    }

    public FunctionalIterator<ConceptMap> execute() {
        return context.options().parallel() ? executeParallel() : executeSerial();
    }

    private FunctionalIterator<ConceptMap> executeParallel() {
        List<? extends List<? extends ConceptMap>> lists = matcher.execute(context).toLists(PARALLELISATION_SPLIT_MIN, PARALLELISATION_FACTOR);
        assert !lists.isEmpty();
        List<ConceptMap> updates;
        if (lists.size() == 1) updates = iterate(lists.get(0)).map(this::executeUpdate).toList();
        else updates = produce(async(
                iterate(lists).map(list -> iterate(list).map(this::executeUpdate)), PARALLELISATION_FACTOR
        ), Either.first(EXHAUSTIVE), async1()).toList();
        return iterate(updates);
    }

    private FunctionalIterator<ConceptMap> executeSerial() {
        List<? extends ConceptMap> matches = matcher.execute(context).onError(conceptMgr::exception).toList();
        List<ConceptMap> answers = iterate(matches).map(this::executeUpdate).toList();
        return iterate(answers);
    }

    private ConceptMap executeUpdate(ConceptMap matched) {
        new Deleter.Operation(matched, deleteVariables).executeInPlace();
        return new Inserter.Operation(conceptMgr, matched, insertVariables).execute();
    }
}
