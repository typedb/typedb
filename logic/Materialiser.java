package com.vaticle.typedb.core.logic;

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concept.thing.Attribute;
import com.vaticle.typedb.core.concept.thing.Relation;
import com.vaticle.typedb.core.concept.thing.Thing;
import com.vaticle.typedb.core.concept.type.AttributeType;
import com.vaticle.typedb.core.logic.Rule.Conclusion;
import com.vaticle.typedb.core.pattern.constraint.thing.ValueConstraint;
import com.vaticle.typedb.core.traversal.RelationTraversal;
import com.vaticle.typedb.core.traversal.TraversalEngine;
import com.vaticle.typedb.core.traversal.common.Identifier;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.traversal.common.Identifier.Variable.anon;

public class Materialiser {

    public static Optional<Map<Identifier.Variable, Concept>> materialiseAndBind(
            Conclusion conclusion, ConceptMap whenConcepts, TraversalEngine traversalEng, ConceptManager conceptMgr
    ) {
        return Materialiser.materialise(conclusion.materialisable(whenConcepts, conceptMgr), traversalEng, conceptMgr)
                .map(materialisation -> materialisation.bindToConclusion(conclusion, whenConcepts));
    }

    /**
     * Perform a put operation on the `then` of the rule. This may insert a new fact, or return an existing inferred fact
     *
     * @param materialisable - the concepts that satisfy the `when` of the rule. All named `then` variables must be in this map
     * @param traversalEng - used to perform a traversal to find preexisting conclusions
     * @param conceptMgr   - used to insert the conclusion if it doesn't already exist
     * @return - an inference if it either was, or could have been, inferred by this conclusion
     */
    public static Optional<Materialisation> materialise(Conclusion.Materialisable materialisable, TraversalEngine traversalEng,
                                                        ConceptManager conceptMgr) {
        if (materialisable.isRelation()) {
            return materialise(materialisable.asRelation(), traversalEng, conceptMgr);
        } else if (materialisable.isHasExplicit()) {
            return materialise(materialisable.asHasExplicit());
        } else if (materialisable.isHasVariable()) {
            return materialise(materialisable.asHasVariable());
        } else {
            throw TypeDBException.of(ILLEGAL_STATE);
        }
    }

    public static Optional<Materialisation> materialise(Conclusion.Has.Explicit.Materialisable materialisable) {
        Attribute attribute = getAttribute(materialisable.attrType(), materialisable.value())
                .orElseGet(() -> putAttribute(materialisable.attrType(), materialisable.value()));
        if (materialisable.owner().hasNonInferred(attribute)) return Optional.empty();
        else {
            materialisable.owner().setHas(attribute, true);
            return Optional.of(new Materialisation.Has.Explicit(
                    materialisable.owner(), materialisable.attrType(), attribute)
            );
        }
    }

    private static Optional<Attribute> getAttribute(AttributeType attrType, ValueConstraint<?> value) {
        if (attrType.isDateTime()) return Optional.ofNullable(attrType.asDateTime().get(value.asDateTime().value()));
        else if (attrType.isBoolean()) return Optional.ofNullable(attrType.asBoolean().get(value.asBoolean().value()));
        else if (attrType.isDouble()) return Optional.ofNullable(attrType.asDouble().get(value.asDouble().value()));
        else if (attrType.isLong()) return Optional.ofNullable(attrType.asLong().get(value.asLong().value()));
        else if (attrType.isString()) return Optional.ofNullable(attrType.asString().get(value.asString().value()));
        else throw TypeDBException.of(ILLEGAL_STATE);
    }

    private static Attribute putAttribute(AttributeType attrType, ValueConstraint<?> value) {
        if (attrType.isDateTime()) return attrType.asDateTime().put(value.asDateTime().value(), true);
        else if (attrType.isBoolean()) return attrType.asBoolean().put(value.asBoolean().value(), true);
        else if (attrType.isDouble()) return attrType.asDouble().put(value.asDouble().value(), true);
        else if (attrType.isLong()) return attrType.asLong().put(value.asLong().value(), true);
        else if (attrType.isString()) return attrType.asString().put(value.asString().value(), true);
        else throw TypeDBException.of(ILLEGAL_STATE);
    }

    public static Optional<Materialisation> materialise(Conclusion.Has.Variable.Materialisable materialisable) {
        Thing owner = materialisable.owner();
        Attribute attribute = materialisable.attribute();
        if (owner.hasNonInferred(attribute)) return Optional.empty();
        else owner.setHas(attribute, true);
        return Optional.of(new Materialisation.Has.Variable(owner, attribute));
    }

    public static Optional<Materialisation> materialise(
            Conclusion.Relation.Materialisable materialisable, TraversalEngine traversalEng, ConceptManager conceptMgr
    ) {
        FunctionalIterator<Relation> existingRelations = matchRelation(materialisable, traversalEng, conceptMgr);
        if (!existingRelations.hasNext()) {
            return Optional.of(new Materialisation.Relation(insert(materialisable)));
        } else {
            while (existingRelations.hasNext()) {
                Relation preexisting = existingRelations.next();
                if (!preexisting.isInferred()) return Optional.empty();
                else {
                    if (insertable(preexisting, materialisable)) {
                        return Optional.of(new Materialisation.Relation(preexisting));
                    }
                }
            }
            return Optional.empty();
        }
    }

    private static boolean insertable(Relation inserted, Conclusion.Relation.Materialisable materialisable) {
        if (!inserted.getType().getLabel().equals(materialisable.relationType().getLabel())) return false;
        Map<Pair<String, Concept>, Integer> relationMap = new HashMap<>();
        materialisable.players().forEach((rp, numOccurrences) -> {
            relationMap.put(new Pair<>(rp.first().getLabel().name(), rp.second()), numOccurrences);
        });
        Map<Pair<String, Concept>, Integer> insertedMap = new HashMap<>();
        inserted.getPlayersByRoleType().forEach((role, players) -> {
            players.forEach(player -> {
                Pair<String, Concept> pair = new Pair<>(role.getLabel().name(), player);
                insertedMap.merge(pair, 1, (p, count) -> count + 1);
            });
        });
        return relationMap.equals(insertedMap);
    }

    private static FunctionalIterator<Relation> matchRelation(
            Conclusion.Relation.Materialisable materialisable, TraversalEngine traversalEng, ConceptManager conceptMgr
    ) {
        AtomicInteger i = new AtomicInteger();
        Identifier.Variable.Retrievable relationId = anon(i.get());
        RelationTraversal traversal = new RelationTraversal(relationId, set(materialisable.relationType().getLabel())); // TODO include inheritance
        materialisable.players().forEach((rp, numOccurrences) -> {
            Identifier.Variable.Anonymous anonVar = anon(i.addAndGet(1));
            for (int j = 1; j <= numOccurrences; j++) {
                traversal.player(anonVar, rp.second().asThing().getIID(), set(rp.first().getLabel())); // TODO include inheritance
            }
        });
        return traversalEng.iterator(traversal).map(conceptMgr::conceptMap)
                .map(conceptMap -> conceptMap.get(relationId).asRelation());
    }

    private static Relation insert(Conclusion.Relation.Materialisable materialisable) {
        Relation relation = materialisable.relationType().create(true);
        materialisable.players().forEach((rp, numOccurrences) -> {
            for (int i = 1; i <= numOccurrences; i++) {
                relation.addPlayer(rp.first(), rp.second(), true);
            }
        });
        return relation;
    }

    public static abstract class Materialisation {

        public abstract Map<Identifier.Variable, Concept> bindToConclusion(Conclusion conclusion, ConceptMap whenConcepts);

        public static class Relation extends Materialisation {
            private final com.vaticle.typedb.core.concept.thing.Relation relation;

            Relation(com.vaticle.typedb.core.concept.thing.Relation relation) {
                this.relation = relation;
            }

            @Override
            public Map<Identifier.Variable, Concept> bindToConclusion(Conclusion conclusion, ConceptMap whenConcepts) {
                assert conclusion.isRelation();
                return conclusion.asRelation().thenConcepts(relation, whenConcepts);
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                Relation relation1 = (Relation) o;
                return relation.equals(relation1.relation);
            }

            @Override
            public int hashCode() {
                return Objects.hash(relation);
            }
        }

        public static abstract  class Has extends Materialisation {

            public static class Explicit extends Has {

                private final Thing owner;
                private final AttributeType attrType;
                private final Attribute attribute;

                public Explicit(Thing owner, AttributeType attrType, Attribute attribute) {
                    this.owner = owner;
                    this.attrType = attrType;
                    this.attribute = attribute;
                }

                @Override
                public Map<Identifier.Variable, Concept> bindToConclusion(Conclusion conclusion, ConceptMap whenConcepts) {
                    assert conclusion.isExplicitHas();
                    return conclusion.asExplicitHas().thenConcepts(owner, attrType, attribute, whenConcepts);
                }

                @Override
                public boolean equals(Object o) {
                    if (this == o) return true;
                    if (o == null || getClass() != o.getClass()) return false;
                    Explicit explicit = (Explicit) o;
                    return owner.equals(explicit.owner) &&
                            attrType.equals(explicit.attrType) &&
                            attribute.equals(explicit.attribute);
                }

                @Override
                public int hashCode() {
                    return Objects.hash(owner, attrType, attribute);
                }
            }

            public static class Variable extends Has {

                private final Thing owner;
                private final Attribute attribute;

                public Variable(Thing owner, Attribute attribute) {
                    this.owner = owner;
                    this.attribute = attribute;
                }

                @Override
                public Map<Identifier.Variable, Concept> bindToConclusion(Conclusion conclusion, ConceptMap whenConcepts) {
                    assert conclusion.isVariableHas();
                    return conclusion.asVariableHas().thenConcepts(owner, attribute, whenConcepts);
                }

                @Override
                public boolean equals(Object o) {
                    if (this == o) return true;
                    if (o == null || getClass() != o.getClass()) return false;
                    Variable variable = (Variable) o;
                    return owner.equals(variable.owner) &&
                            attribute.equals(variable.attribute);
                }

                @Override
                public int hashCode() {
                    return Objects.hash(owner, attribute);
                }
            }
        }
    }
}
