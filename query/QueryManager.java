/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.query;

import grabl.tracing.client.GrablTracingThreadStatic.ThreadTrace;
import grakn.core.common.exception.GraknException;
import grakn.core.common.iterator.ResourceIterator;
import grakn.core.common.parameters.Context;
import grakn.core.common.parameters.Options;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.answer.ConceptMapGroup;
import grakn.core.concept.answer.Numeric;
import grakn.core.concept.answer.NumericGroup;
import grakn.core.concept.thing.Attribute;
import grakn.core.logic.LogicManager;
import grakn.core.pattern.Disjunction;
import grakn.core.reasoner.Reasoner;
import graql.lang.common.GraqlArg;
import graql.lang.pattern.variable.Reference;
import graql.lang.query.GraqlDefine;
import graql.lang.query.GraqlDelete;
import graql.lang.query.GraqlInsert;
import graql.lang.query.GraqlMatch;
import graql.lang.query.GraqlUndefine;
import graql.lang.query.builder.Sortable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;
import java.util.Set;

import static grabl.tracing.client.GrablTracingThreadStatic.traceOnThread;
import static grakn.common.collection.Collections.list;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.Internal.UNIMPLEMENTED;
import static grakn.core.common.exception.ErrorMessage.ThingRead.INVALID_THING_CASTING;
import static grakn.core.common.exception.ErrorMessage.ThingRead.SORT_ATTRIBUTE_NOT_COMPARABLE;
import static grakn.core.common.exception.ErrorMessage.ThingRead.SORT_VARIABLE_NOT_ATTRIBUTE;
import static grakn.core.common.exception.ErrorMessage.Transaction.SESSION_DATA_VIOLATION;
import static grakn.core.common.exception.ErrorMessage.Transaction.SESSION_SCHEMA_VIOLATION;
import static grakn.core.common.iterator.Iterators.iterate;

public class QueryManager {

    private static final Logger LOG = LoggerFactory.getLogger(QueryManager.class);

    private static final String TRACE_PREFIX = "query.";
    private final LogicManager logicMgr;
    private final Reasoner reasoner;
    private final ConceptManager conceptMgr;
    private final Context.Transaction transactionCtx;

    public QueryManager(ConceptManager conceptMgr, LogicManager logicMgr, Reasoner reasoner, Context.Transaction transactionCtx) {
        this.conceptMgr = conceptMgr;
        this.logicMgr = logicMgr;
        this.reasoner = reasoner;
        this.transactionCtx = transactionCtx;
    }

    public ResourceIterator<ConceptMap> match(GraqlMatch query) {
        return match(query, true, new Options.Query());
    }

    public ResourceIterator<ConceptMap> match(GraqlMatch query, boolean isParallel) {
        return match(query, isParallel, new Options.Query());
    }

    public ResourceIterator<ConceptMap> match(GraqlMatch query, Options.Query options) {
        return match(query, true, options);
    }

    public ResourceIterator<ConceptMap> match(GraqlMatch query, boolean isParallel, Options.Query options) {
        // TODO: Note that Query Options are not yet utilised during match query
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "match")) {
            Disjunction disjunction = Disjunction.create(query.conjunction().normalise());
            return filter(reasoner.execute(disjunction, isParallel), query);
        } catch (GraknException exception) {
            throw conceptMgr.exception(exception);
        } catch (Exception exception) {
            throw conceptMgr.exception(exception);
        }
    }

    private ResourceIterator<ConceptMap> filter(ResourceIterator<ConceptMap> answers, GraqlMatch query) {
        if (!query.filter().isEmpty()) {
            Set<Reference.Name> vars = iterate(query.filter()).map(f -> f.reference().asName()).toSet();
            answers = answers.map(a -> a.filter(vars)).distinct();
        }
        if (query.sort().isPresent()) {
            answers = sort(answers, query.sort().get());
        }
        if (query.offset().isPresent()) {
            answers = answers.offset(query.offset().get());
        }
        if (query.limit().isPresent()) {
            answers = answers.limit(query.limit().get());
        }
        return answers;
    }

    private ResourceIterator<ConceptMap> sort(ResourceIterator<ConceptMap> answers, Sortable.Sorting sorting) {
        // TODO: Replace this temporary implementation of Graql Match Sort query
        Reference.Name var = sorting.var().reference().asName();
        Comparator<ConceptMap> comparator = (answer1, answer2) -> {
            Attribute att1, att2;
            try {
                att1 = answer1.get(var).asAttribute();
                att2 = answer2.get(var).asAttribute();
            } catch (GraknException e) {
                if (e.code().isPresent() || e.code().get().equals(INVALID_THING_CASTING.code())) {
                    throw GraknException.of(SORT_VARIABLE_NOT_ATTRIBUTE, var);
                } else {
                    throw e;
                }
            }

            if (!att1.getType().getValueType().comparables().contains(att2.getType().getValueType())) {
                throw GraknException.of(SORT_ATTRIBUTE_NOT_COMPARABLE,
                                        att1.getType().getValueType(), att2.getType().getValueType());
            }
            if (att1.isString()) {
                return att1.asString().getValue().compareToIgnoreCase(att2.asString().getValue());
            } else if (att1.isBoolean()) {
                return att1.asBoolean().getValue().compareTo(att2.asBoolean().getValue());
            } else if (att1.isLong() && att2.isLong()) {
                return att1.asLong().getValue().compareTo(att2.asLong().getValue());
            } else if (att1.isDouble() || att2.isDouble()) {
                Double double1 = att1.isLong() ? att1.asLong().getValue() : att1.asDouble().getValue();
                Double double2 = att2.isLong() ? att2.asLong().getValue() : att2.asDouble().getValue();
                return double1.compareTo(double2);
            } else if (att1.isDateTime()) {
                return (att1.asDateTime().getValue()).compareTo(att2.asDateTime().getValue());
            } else {
                throw GraknException.of(ILLEGAL_STATE);
            }
        };
        comparator = (sorting.order() == GraqlArg.Order.DESC) ? comparator.reversed() : comparator;
        return iterate(answers.stream().sorted(comparator).iterator());
    }

    public Numeric match(GraqlMatch.Aggregate query) {
        return match(query, true, new Options.Query());
    }

    public Numeric match(GraqlMatch.Aggregate query, boolean isParallel) {
        return match(query, isParallel, new Options.Query());
    }

    public Numeric match(GraqlMatch.Aggregate query, Options.Query options) {
        return match(query, true, options);
    }

    public Numeric match(GraqlMatch.Aggregate query, boolean isParallel, Options.Query options) {
        throw GraknException.of(UNIMPLEMENTED);
    }

    public ResourceIterator<ConceptMapGroup> match(GraqlMatch.Group query) {
        return match(query, true, new Options.Query());
    }

    public ResourceIterator<ConceptMapGroup> match(GraqlMatch.Group query, boolean isParallel) {
        return match(query, isParallel, new Options.Query());
    }

    public ResourceIterator<ConceptMapGroup> match(GraqlMatch.Group query, Options.Query options) {
        return match(query, true, options);
    }

    public ResourceIterator<ConceptMapGroup> match(GraqlMatch.Group query, boolean isParallel, Options.Query options) {
        throw GraknException.of(UNIMPLEMENTED);
    }

    public ResourceIterator<NumericGroup> match(GraqlMatch.Group.Aggregate query) {
        return match(query, true, new Options.Query());
    }

    public ResourceIterator<NumericGroup> match(GraqlMatch.Group.Aggregate query, boolean isParallel) {
        return match(query, isParallel, new Options.Query());
    }

    public ResourceIterator<NumericGroup> match(GraqlMatch.Group.Aggregate query, Options.Query options) {
        return match(query, true, options);
    }

    public ResourceIterator<NumericGroup> match(GraqlMatch.Group.Aggregate query, boolean isParallel, Options.Query options) {
        throw GraknException.of(UNIMPLEMENTED);
    }

    public ResourceIterator<ConceptMap> insert(GraqlInsert query) {
        return insert(query, new Options.Query());
    }

    public ResourceIterator<ConceptMap> insert(GraqlInsert query, Options.Query options) {
        if (transactionCtx.sessionType().isSchema()) throw conceptMgr.exception(SESSION_SCHEMA_VIOLATION);
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "insert")) {
            final Context.Query context = new Context.Query(transactionCtx, options);
            if (query.match().isPresent()) {
                List<ConceptMap> matched = match(query.match().get(), options).toList();
                return iterate(iterate(matched).map(answer -> Inserter.create(
                        conceptMgr, query.variables(), answer, context
                ).execute()).toList());
            } else {
                return iterate(list(Inserter.create(conceptMgr, query.variables(), context).execute()));
            }
        } catch (GraknException exception) {
            throw conceptMgr.exception(exception);
        } catch (Exception exception) {
            throw conceptMgr.exception(exception);
        }
    }

    public void delete(GraqlDelete query) {
        delete(query, new Options.Query());
    }

    public void delete(GraqlDelete query, Options.Query options) {
        if (transactionCtx.sessionType().isSchema()) throw conceptMgr.exception(SESSION_SCHEMA_VIOLATION);
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "delete")) {
            final Context.Query context = new Context.Query(transactionCtx, options);
            final List<ConceptMap> matched = match(query.match(), options).toList();
            matched.forEach(existing -> Deleter.create(conceptMgr, query.variables(), existing, context).execute());
        } catch (GraknException exception) {
            throw conceptMgr.exception(exception);
        } catch (Exception exception) {
            throw conceptMgr.exception(exception);
        }
    }

    public void define(GraqlDefine query) {
        if (transactionCtx.sessionType().isData()) throw conceptMgr.exception(SESSION_DATA_VIOLATION);
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "define")) {
            Definer.create(conceptMgr, logicMgr, query.variables(), query.rules()).execute();
        } catch (GraknException exception) {
            throw conceptMgr.exception(exception);
        } catch (Exception exception) {
            throw conceptMgr.exception(exception);
        }
    }

    public void undefine(GraqlUndefine query) {
        if (transactionCtx.sessionType().isData()) throw conceptMgr.exception(SESSION_DATA_VIOLATION);
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "undefine")) {
            Undefiner.create(conceptMgr, logicMgr, query.variables(), query.rules()).execute();
        } catch (GraknException exception) {
            throw conceptMgr.exception(exception);
        } catch (Exception exception) {
            throw conceptMgr.exception(exception);
        }
    }
}
