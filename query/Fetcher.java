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
import com.vaticle.typedb.core.concept.type.ThingType;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Projection.ILLEGAL_ATTRIBUTE_PROJECTION_ATTRIBUTE_TYPE_INVALID;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Projection.ILLEGAL_ATTRIBUTE_PROJECTION_TYPES_NOT_OWNED;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Projection.ILLEGAL_ATTRIBUTE_PROJECTION_TYPE_VARIABLE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Projection.PROJECTION_VARIABLE_UNBOUND;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Projection.PROJECTION_VARIABLE_UNNAMED;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Projection.SUBQUERY_UNBOUNDED;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Projection.VARIABLE_PROJECTION_CONCEPT_NOT_READABLE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.common.parameters.Concept.Transitivity.TRANSITIVE;
import static java.util.Collections.emptySet;

public class Fetcher {

    private final Reasoner reasoner;
    private final ConceptManager conceptMgr;
    private final Context.Query context;
    private final Disjunction match;
    private final TypeQLQuery.Modifiers modifiers;
    private final List<Identifier.Variable.Name> filter;
    private final List<Projection> projections;

    private Fetcher(Reasoner reasoner, ConceptManager conceptMgr, Disjunction match,
                    TypeQLQuery.Modifiers modifiers, List<Projection> projections, Context.Query context) {
        this.reasoner = reasoner;
        this.conceptMgr = conceptMgr;
        this.modifiers = modifiers;
        this.match = match;
        this.projections = projections;
        this.filter = iterate(this.projections).flatMap(Projection::namedVariables)
                .filter(varID -> this.match.sharedVariables().contains(varID))
                .toList();
        if (this.modifiers.sort().isPresent()) {
            iterate(modifiers.sort().get().variables()).map(v -> Identifier.Variable.of(v.reference().asName()))
                    .forEachRemaining(filter::add);
        }
        this.context = context;
    }

    public static Fetcher create(Reasoner reasoner, ConceptManager conceptMgr, TypeQLFetch query, Context.Query context) {
        return create(reasoner, conceptMgr, emptySet(), query, context);
    }

    public static Fetcher create(Reasoner reasoner, ConceptManager conceptMgr, Set<Identifier.Variable.Name> bounds,
                                 TypeQLFetch query, Context.Query context) {
        Disjunction match = Disjunction.create(query.match().conjunction().normalise());
        List<Projection> projections = iterate(query.projections())
                .map(p -> Projection.create(reasoner, conceptMgr, context, match, bounds, p)).toList();
        return new Fetcher(reasoner, conceptMgr, match, query.modifiers(), projections, context);
    }

    public Disjunction match() {
        return match;
    }

    public FunctionalIterator<Identifier.Variable.Name> namedVariables() {
        return iterate(match.sharedVariables())
                .link(iterate(projections).flatMap(p ->
                        iterate(p.namedVariables()).map(typeQLVar -> Identifier.Variable.of(typeQLVar.reference().asName()))
                ));
    }

    public FunctionalIterator<ReadableConceptTree> execute() {
        return execute(context, new ConceptMap());
    }

    public FunctionalIterator<ReadableConceptTree> execute(ConceptMap bindings) {
        return execute(context, bindings);
    }

    FunctionalIterator<ReadableConceptTree> execute(Context.Query context, ConceptMap bindings) {
        FunctionalIterator<? extends ConceptMap> answers = reasoner.execute(match, filter, modifiers, context, bindings);
        // at this point, type inference is guaranteed to have run
        projections.forEach(p -> p.validateTypes(conceptMgr, match));
        return answers.map(cm -> executeProjections(cm.merge(bindings)));
    }

    private ReadableConceptTree executeProjections(ConceptMap concepts) {
        ReadableConceptTree.Node.Map root = new ReadableConceptTree.Node.Map();
        projections.forEach(projection ->
                root.add(projection.key(), projection.execute(reasoner, conceptMgr, context, concepts))
        );
        return new ReadableConceptTree(root);
    }

    private static abstract class Projection {

        private static Projection create(Reasoner reasoner, ConceptManager conceptMgr, Context.Query context, Disjunction match,
                                         Set<Identifier.Variable.Name> bounds, TypeQLFetch.Projection typeQLProjection) {
            if (typeQLProjection.isVariable()) {
                return Variable.create(match, typeQLProjection.asVariable().key());
            } else if (typeQLProjection.isAttribute()) {
                return Attribute.create(
                        conceptMgr, match, typeQLProjection.asAttribute().key(),
                        typeQLProjection.asAttribute().attributes()
                );
            } else if (typeQLProjection.isSubquery()) {
                Set<Identifier.Variable.Name> boundsMerged = new HashSet<>(match.sharedVariables());
                boundsMerged.addAll(bounds);
                return Subquery.create(reasoner, conceptMgr, context, boundsMerged,
                        typeQLProjection.asSubquery().key(), typeQLProjection.asSubquery().subquery()
                );
            } else throw TypeDBException.of(ILLEGAL_STATE);
        }

        public abstract String key();

        public abstract FunctionalIterator<Identifier.Variable.Name> namedVariables();

        abstract ReadableConceptTree.Node execute(Reasoner reasoner, ConceptManager conceptMgr, Context.Query context,
                                                  ConceptMap concepts);

        abstract void validateTypes(ConceptManager conceptMgr, Disjunction match);

        private static class Variable extends Projection {

            private final TypeQLVariable variable;
            private final TypeQLFetch.Key.Label label;

            private Variable(TypeQLFetch.Key.Var key) {
                this.variable = key.typeQLVar();
                this.label = key.label().orElse(null);
            }

            public static Projection create(Disjunction match, TypeQLFetch.Key.Var key) {
                if (!key.typeQLVar().reference().isName()) {
                    throw TypeDBException.of(PROJECTION_VARIABLE_UNNAMED, key.typeQLVar());
                } else if (!match.sharedVariables().contains(Identifier.Variable.of(key.typeQLVar().reference().asName()))) {
                    throw TypeDBException.of(PROJECTION_VARIABLE_UNBOUND, key.typeQLVar());
                }
                return new Variable(key);
            }

            @Override
            public String key() {
                return label == null ? this.variable.reference().name() : label.label();
            }

            @Override
            public FunctionalIterator<Identifier.Variable.Name> namedVariables() {
                return iterate(Identifier.Variable.of(variable.reference().asName()));
            }

            @Override
            void validateTypes(ConceptManager conceptMgr, Disjunction match) {
                // pass
            }

            @Override
            ReadableConceptTree.Node.Leaf<? extends Concept.Readable> execute(Reasoner reasoner, ConceptManager conceptMgr,
                                                                              Context.Query context, ConceptMap concepts) {
                // runtime type check since polymorphic queries may range over non-readable concepts
                Concept concept = concepts.get(variable);
                if (!(concept instanceof Concept.Readable)) {
                    throw TypeDBException.of(VARIABLE_PROJECTION_CONCEPT_NOT_READABLE, key());
                }
                return new ReadableConceptTree.Node.Leaf<>((Concept.Readable) concept);
            }
        }

        private static class Attribute extends Projection {

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
                    throw TypeDBException.of(PROJECTION_VARIABLE_UNNAMED, key.typeQLVar());
                } else {
                    Identifier.Variable.Name id = Identifier.Variable.of(key.typeQLVar().reference().asName());
                    if (!match.sharedVariables().contains(id)) {
                        throw TypeDBException.of(PROJECTION_VARIABLE_UNBOUND, key.typeQLVar());
                    } else if (iterate(match.conjunctions()).flatMap(conj -> iterate(conj.variables()))
                            .anyMatch(var -> var.id().equals(id) && !var.isThing())) {
                        throw TypeDBException.of(ILLEGAL_ATTRIBUTE_PROJECTION_TYPE_VARIABLE, key.typeQLVar());
                    }
                }

                List<AttributeFetch> attrs = new ArrayList<>();
                for (Pair<Reference.Label, TypeQLFetch.Key.Label> pair : attributes) {
                    AttributeType attributeType = conceptMgr.getAttributeType(pair.first().label());
                    if (attributeType == null) {
                        throw TypeDBException.of(ILLEGAL_ATTRIBUTE_PROJECTION_ATTRIBUTE_TYPE_INVALID, key.typeQLVar(), pair.first());
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
            public FunctionalIterator<Identifier.Variable.Name> namedVariables() {
                return iterate(Identifier.Variable.of(keyVariable.reference().asName()));
            }

            @Override
            ReadableConceptTree.Node.Map execute(Reasoner reasoner, ConceptManager conceptMgr,
                                                 Context.Query context, ConceptMap concepts) {
                ReadableConceptTree.Node.Map entries = new ReadableConceptTree.Node.Map();
                Thing thing = concepts.get(keyVariable).asThing();
                entries.add(Concept.Readable.KEY_TYPE, new ReadableConceptTree.Node.Leaf<>(thing.getType()));
                attributeFetches.forEach(attrFetch -> entries.add(
                        attrFetch.name(),
                        new ReadableConceptTree.Node.List(
                                thing.getHas(attrFetch.attributeTypes(), emptySet()).map(ReadableConceptTree.Node.Leaf::new).toList()
                        )
                ));
                return entries;
            }

            @Override
            void validateTypes(ConceptManager conceptMgr, Disjunction match) {
                assert keyVariable.isNamed();
                Identifier.Variable.Name keyID = Identifier.Variable.of(keyVariable.reference().asName());
                // for each attribute type, check each possible type from the 'match' variable can own the attribute type or a subtype of it
                iterate(match.getTypes(keyID)).forEachRemaining(label -> {
                    ThingType type = conceptMgr.getType(label).asThingType();
                    Set<? extends AttributeType> ownedTypes = type.getOwnedAttributes(TRANSITIVE);
                    // one of the subtypes must be in the set of owned types
                    iterate(this.attributeFetches).forEachRemaining(attributeFetch -> {
                        if (iterate(attributeFetch.attributeTypes).noneMatch(ownedTypes::contains)) {
                            throw TypeDBException.of(ILLEGAL_ATTRIBUTE_PROJECTION_TYPES_NOT_OWNED,
                                    keyVariable, attributeFetch.name(), keyVariable, label);
                        }
                    });
                });
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

        private static abstract class Subquery extends Projection {

            private final TypeQLFetch.Key.Label key;

            private Subquery(TypeQLFetch.Key.Label key) {
                this.key = key;
            }

            public static Projection create(Reasoner reasoner, ConceptManager conceptMgr, Context.Query context,
                                            Set<Identifier.Variable.Name> bounds, TypeQLFetch.Key.Label key,
                                            Either<TypeQLFetch, TypeQLGet.Aggregate> subquery) {
                Set<TypeQLVariable> subqueryVariables = subquery.isFirst() ?
                        subquery.first().match().namedVariables() :
                        subquery.second().get().match().namedVariables();
                if (iterate(subqueryVariables).noneMatch(v ->
                        v.isNamed() && bounds.contains(Identifier.Variable.of(v.reference().asName()))
                )) {
                    throw TypeDBException.of(SUBQUERY_UNBOUNDED, key.label());
                }

                if (subquery.isFirst()) {
                    return SubFetch.create(reasoner, conceptMgr, context, bounds, key, subquery.first());
                } else return SubGetAggregate.create(reasoner, conceptMgr, context, bounds, key, subquery.second());
            }

            @Override
            public String key() {
                return key.label();
            }

            @Override
            void validateTypes(ConceptManager conceptMgr, Disjunction match) {
                // TODO: we could try to propagate the set of types into the projections and validate that the queries
                //       may be semantically valid, rather than waiting until execution time.
            }

            private static class SubFetch extends Subquery {

                private final Fetcher fetcher;

                private SubFetch(TypeQLFetch.Key.Label key, Fetcher fetcher) {
                    super(key);
                    this.fetcher = fetcher;
                }

                private static SubFetch create(Reasoner reasoner, ConceptManager conceptMgr, Context.Query context,
                                               Set<Identifier.Variable.Name> bounds, TypeQLFetch.Key.Label key, TypeQLFetch subquery) {
                    return new SubFetch(key, Fetcher.create(reasoner, conceptMgr, bounds, subquery, context));
                }

                @Override
                public FunctionalIterator<Identifier.Variable.Name> namedVariables() {
                    return fetcher.namedVariables();
                }

                @Override
                ReadableConceptTree.Node execute(Reasoner reasoner, ConceptManager conceptMgr, Context.Query context, ConceptMap concepts) {
                    return new ReadableConceptTree.Node.List(fetcher.execute(concepts).map(ReadableConceptTree::root).toList());
                }
            }

            private static class SubGetAggregate extends Subquery {

                private final Getter.Aggregator getAggregator;

                public SubGetAggregate(TypeQLFetch.Key.Label key, Getter.Aggregator getAggregator) {
                    super(key);
                    this.getAggregator = getAggregator;
                }

                public static SubGetAggregate create(Reasoner reasoner, ConceptManager conceptMgr, Context.Query context,
                                                     Set<Identifier.Variable.Name> bounds, TypeQLFetch.Key.Label key,
                                                     TypeQLGet.Aggregate getAggregate) {
                    if (iterate(getAggregate.get().match().namedVariables())
                            .noneMatch(v -> bounds.contains(Identifier.Variable.of(v.reference().asName())))) {
                        throw TypeDBException.of(SUBQUERY_UNBOUNDED, key.label());
                    }
                    return new SubGetAggregate(key, Getter.create(reasoner, conceptMgr, getAggregate, context));
                }

                @Override
                public FunctionalIterator<Identifier.Variable.Name> namedVariables() {
                    return iterate(getAggregator.getter().disjunction().sharedVariables());
                }

                @Override
                ReadableConceptTree.Node execute(Reasoner reasoner, ConceptManager conceptMgr, Context.Query context, ConceptMap concepts) {
                    return new ReadableConceptTree.Node.Leaf<>(getAggregator.execute(concepts).orElse(null));
                }
            }
        }
    }
}
