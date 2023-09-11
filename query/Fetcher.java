/*
 * Copyright (C) 2023 Vaticle
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
import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.parameters.Context;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concept.answer.ReadableConceptTree;
import com.vaticle.typedb.core.concept.thing.Thing;
import com.vaticle.typedb.core.concept.type.AttributeType;
import com.vaticle.typedb.core.pattern.Disjunction;
import com.vaticle.typedb.core.reasoner.Reasoner;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typeql.lang.common.Reference;
import com.vaticle.typeql.lang.common.TypeQLVariable;
import com.vaticle.typeql.lang.query.TypeQLFetch;
import com.vaticle.typeql.lang.query.TypeQLGet;
import com.vaticle.typeql.lang.query.TypeQLQuery;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class Fetcher {

    private final Reasoner reasoner;
    private final ConceptManager conceptMgr;
    private final Context.Query context;
    private final Disjunction match;
    private final TypeQLQuery.Modifiers modifiers;
    private final List<TypeQLVariable> filter;
    private final List<Projection> projections;

    private Fetcher(Reasoner reasoner, ConceptManager conceptMgr, Disjunction match,
                    TypeQLQuery.Modifiers modifiers, List<Projection> projections, @Nullable Context.Query context) {
        this.reasoner = reasoner;
        this.conceptMgr = conceptMgr;
        this.modifiers = modifiers;
        this.match = match;
        this.projections = projections;
        this.filter = iterate(this.projections).flatMap(Projection::namedVariables)
                .filter(var -> this.match.sharedVariables().contains(Identifier.Variable.of(var.reference().asName())))
                .toList();
        if (this.modifiers.sort().isPresent()) filter.addAll(modifiers.sort().get().variables());
        this.context = context;
    }

    public static Fetcher create(Reasoner reasoner, ConceptManager conceptMgr, TypeQLFetch query) {
        return create(reasoner, conceptMgr, query, null);
    }

    public static Fetcher create(Reasoner reasoner, ConceptManager conceptMgr, TypeQLFetch query, Context.Query context) {
        Disjunction match = Disjunction.create(query.match().conjunction().normalise());
        List<Projection> projections = iterate(query.projections())
                .map(p -> Projection.create(conceptMgr, match, p)).toList();
        return new Fetcher(reasoner, conceptMgr, match, query.modifiers(), projections, context);
    }

    public Disjunction match() {
        return match;
    }

    public FunctionalIterator<ReadableConceptTree> execute() {
        assert context != null;
        return execute(context);
    }

    FunctionalIterator<ReadableConceptTree> execute(Context.Query context) {
        return reasoner.execute(match, filter, modifiers, context).map(this::executeProjections);
    }

    private ReadableConceptTree executeProjections(ConceptMap matchAnswer) {
        ReadableConceptTree.Node.Map root = new ReadableConceptTree.Node.Map();
        projections.forEach(projection ->
                root.add(projection.key(), projection.execute(reasoner, conceptMgr, context, matchAnswer))
        );
        return new ReadableConceptTree(root);
    }

    private static abstract class Projection {

        private static Projection create(ConceptManager conceptMgr, Disjunction match,
                                         TypeQLFetch.Projection typeQLProjection) {
            if (typeQLProjection.isVariable()) {
                return Variable.create(match, typeQLProjection.asVariable().key());
            } else if (typeQLProjection.isAttribute()) {
                return Attribute.create(
                        conceptMgr, match, typeQLProjection.asAttribute().key(),
                        typeQLProjection.asAttribute().attributes()
                );
            } else if (typeQLProjection.isSubquery()) {
                return Subquery.create(
                        match, typeQLProjection.asSubquery().key(), typeQLProjection.asSubquery().subquery()
                );
            } else throw TypeDBException.of(ILLEGAL_STATE);
        }

        public abstract String key();

        public abstract FunctionalIterator<TypeQLVariable> namedVariables();

        abstract ReadableConceptTree.Node execute(Reasoner reasoner, ConceptManager conceptMgr, Context.Query context,
                                                  ConceptMap matchAnswer);

        private static class Variable extends Projection {

            private final TypeQLVariable variable;
            private final TypeQLFetch.Key.Label label;

            private Variable(TypeQLFetch.Key.Var key) {
                this.variable = key.typeQLVar();
                this.label = key.label().orElse(null);
            }

            public static Projection create(Disjunction match, TypeQLFetch.Key.Var key) {
                if (!key.typeQLVar().reference().isName()) {
                    // TODO useful exception
                    throw new RuntimeException("Projection variable must be named.");
                } else if (!match.sharedVariables().contains(Identifier.Variable.of(key.typeQLVar().reference().asName()))) {
                    // TODO throw useful exception
                    throw new RuntimeException("Projection variable must be bound in the match clause.");
                }

                return new Variable(key);
            }

            @Override
            public String key() {
                return label == null ? this.variable.reference().name() : label.label();
            }

            @Override
            public FunctionalIterator<TypeQLVariable> namedVariables() {
                return iterate(variable);
            }

            @Override
            ReadableConceptTree.Node.Leaf<? extends Concept.Readable> execute(Reasoner reasoner, ConceptManager conceptMgr,
                                                                              Context.Query context, ConceptMap matchAnswer) {
                // runtime type check since polymorphic queries may range over non-readable concepts
                Concept concept = matchAnswer.get(variable);
                if (!(concept instanceof Concept.Readable)) {
                    // TODO throw useful exception
                    throw new RuntimeException("error");
                }
                return new ReadableConceptTree.Node.Leaf<>((Concept.Readable) concept);
            }
        }

        private static class Attribute extends Projection {

            private static final String TYPE_KEY = "type";

            private final TypeQLVariable keyVariable;
            private final TypeQLFetch.Key.Label keyLabel;
            private final List<AttributeFetch> attributeFetches;

            public Attribute(TypeQLVariable key, @Nullable TypeQLFetch.Key.Label keyLabel,
                             List<AttributeFetch> attributeFetches) {
                this.keyVariable = key;
                this.keyLabel = keyLabel;
                this.attributeFetches = attributeFetches;
            }

            public static Projection create(ConceptManager conceptMgr, Disjunction match, TypeQLFetch.Key.Var key,
                                            List<Pair<Reference.Label, TypeQLFetch.Key.Label>> attributes) {
                if (!key.typeQLVar().reference().isName()) {
                    // TODO useful exception
                    throw new RuntimeException("Projection variable must be named.");
                } else {
                    Identifier.Variable.Name id = Identifier.Variable.of(key.typeQLVar().reference().asName());
                    if (!match.sharedVariables().contains(id)) {
                        // TODO throw useful exception
                        throw new RuntimeException("Projection variable must be bound in the match clause.");
                    } else if (iterate(match.conjunctions()).flatMap(conj -> iterate(conj.variables()))
                            .anyMatch(var -> var.id().equals(id) && !var.isThing())) {
                        // TODO proper exception
                        throw new RuntimeException("Projection variable must represent instances and not types or values.");
                    }
                }

                List<AttributeFetch> attrs = new ArrayList<>();
                for (Pair<Reference.Label, TypeQLFetch.Key.Label> pair : attributes) {
                    AttributeType attributeType = conceptMgr.getAttributeType(pair.first().label());
                    if (attributeType == null) {
                        // TODO throw useful exception
                        throw new RuntimeException("Unrecognised attribute type '" + pair.first() + "' in attribute projection from '" + key.typeQLVar());
                    }
                    attrs.add(AttributeFetch.create(attributeType, pair.second()));
                }
                return new Attribute(key.typeQLVar(), key.label().orElse(null), attrs);
            }

            @Override
            public String key() {
                return keyLabel == null ? this.keyVariable.reference().name() : keyLabel.label();
            }

            @Override
            public FunctionalIterator<TypeQLVariable> namedVariables() {
                return iterate(keyVariable);
            }

            @Override
            ReadableConceptTree.Node.Map execute(Reasoner reasoner, ConceptManager conceptMgr,
                                                 Context.Query context, ConceptMap matchAnswer) {
                ReadableConceptTree.Node.Map entries = new ReadableConceptTree.Node.Map();
                Thing thing = matchAnswer.get(keyVariable).asThing();
                entries.add(TYPE_KEY, new ReadableConceptTree.Node.Leaf<>(thing.getType()));
                attributeFetches.forEach(attrFetch -> entries.add(
                        attrFetch.name(),
                        new ReadableConceptTree.Node.List(
                                thing.getHas(attrFetch.attributeTypes()).map(ReadableConceptTree.Node.Leaf::new).toList()
                        )
                ));
                return entries;
            }

            private static class AttributeFetch {

                private final List<? extends AttributeType> attributeTypes;
                private final String name;

                private AttributeFetch(String name, List<? extends AttributeType> attributeTypes) {
                    this.name = name;
                    this.attributeTypes = attributeTypes;
                }

                public static AttributeFetch create(AttributeType attributeType, @Nullable TypeQLFetch.Key.Label label) {
                    List<? extends AttributeType> attributeTypes = attributeType.getSubtypes().toList();
                    if (label == null) return new AttributeFetch(attributeType.getLabel().scopedName(), attributeTypes);
                    else return new AttributeFetch(label.label(), attributeTypes);
                }

                public String name() {
                    return name;
                }

                public List<? extends AttributeType> attributeTypes() {
                    return attributeTypes;
                }
            }
        }

        private static class Subquery extends Projection {

            private final TypeQLFetch.Key.Label key;
            private final Either<TypeQLFetch, TypeQLGet.Aggregate> subquery;

            private Subquery(TypeQLFetch.Key.Label key, Either<TypeQLFetch, TypeQLGet.Aggregate> subquery) {
                this.key = key;
                this.subquery = subquery;
            }

            public static Projection create(Disjunction match, TypeQLFetch.Key.Label key,
                                            Either<TypeQLFetch, TypeQLGet.Aggregate> subquery) {
                Set<TypeQLVariable> subqueryVariables;
                if (subquery.isFirst()) subqueryVariables = subquery.first().match().namedVariables();
                else subqueryVariables = subquery.second().get().match().namedVariables();
                // TODO: how to validate against parent's parent's shared variables effectively? Should probably use provided bounds
                if (iterate(subqueryVariables).noneMatch(v ->
                        v.isNamed() && match.sharedVariables().contains(Identifier.Variable.of(v.reference().asName()))
                )) {
                    // TODO: proper message
                    throw new RuntimeException("Fetch subquery labeled '" + key.label() + "' us not bounded by the match clause.");
                }
                return new Subquery(key, subquery);
            }

            @Override
            public String key() {
                return key.label();
            }

            @Override
            public FunctionalIterator<TypeQLVariable> namedVariables() {
                return subquery.isFirst() ? iterate(subquery.first().match().namedVariables()) :
                        iterate(subquery.second().get().match().namedVariables());
            }

            @Override
            ReadableConceptTree.Node execute(Reasoner reasoner, ConceptManager conceptMgr,
                                             Context.Query context, ConceptMap matchAnswer) {
                // TODO we have to pass the bounds into the recursive query
                if (subquery.isFirst()) {
                    return new ReadableConceptTree.Node.List(
                            Fetcher.create(reasoner, conceptMgr, subquery.first(), context).execute()
                                    .map(ReadableConceptTree::root).toList()
                    );
                } else {
                    // TODO numerics should be replaced by Values
//                    return new ReadableConceptTree.Node.Leaf<>(Getter.create(reasoner, subquery.second(), context).execute());
                    return null;
                }
            }
        }
    }
}
