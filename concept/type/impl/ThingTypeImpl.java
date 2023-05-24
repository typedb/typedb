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

package com.vaticle.typedb.core.concept.type.impl;

import com.vaticle.typedb.core.common.exception.ErrorMessage;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Forwardable;
import com.vaticle.typedb.core.common.parameters.Concept.Transitivity;
import com.vaticle.typedb.core.common.parameters.Order;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.thing.Attribute;
import com.vaticle.typedb.core.concept.thing.Thing;
import com.vaticle.typedb.core.concept.thing.impl.AttributeImpl;
import com.vaticle.typedb.core.concept.thing.impl.EntityImpl;
import com.vaticle.typedb.core.concept.thing.impl.RelationImpl;
import com.vaticle.typedb.core.concept.thing.impl.ThingImpl;
import com.vaticle.typedb.core.concept.type.AttributeType;
import com.vaticle.typedb.core.concept.type.RoleType;
import com.vaticle.typedb.core.concept.type.ThingType;
import com.vaticle.typedb.core.concept.type.Type;
import com.vaticle.typedb.core.encoding.Encoding;
import com.vaticle.typedb.core.graph.edge.TypeEdge;
import com.vaticle.typedb.core.graph.vertex.ThingVertex;
import com.vaticle.typedb.core.graph.vertex.TypeVertex;
import com.vaticle.typeql.lang.common.TypeQLToken;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;

import static com.vaticle.typedb.common.collection.Collections.concatToList;
import static com.vaticle.typedb.common.collection.Collections.pair;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNRECOGNISED_VALUE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.INVALID_UNDEFINE_INHERITED_OWNS;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.INVALID_UNDEFINE_INHERITED_PLAYS;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.INVALID_UNDEFINE_NONEXISTENT_OWNS;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.INVALID_UNDEFINE_NONEXISTENT_PLAYS;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.INVALID_UNDEFINE_OWNS_HAS_INSTANCES;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.INVALID_UNDEFINE_PLAYS_HAS_INSTANCES;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.OVERRIDDEN_OWNED_ATTRIBUTE_TYPE_NOT_SUPERTYPE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.OVERRIDDEN_PLAYED_ROLE_TYPE_NOT_SUPERTYPE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.OVERRIDE_NOT_AVAILABLE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.OWNS_ABSTRACT_ATTRIBUTE_TYPE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.OWNS_ANNOTATION_DECLARATION_INCOMPATIBLE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.OWNS_ANNOTATION_LESS_STRICT_THAN_PARENT;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.OWNS_ANNOTATION_REDECLARATION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.OWNS_ATTRIBUTE_REDECLARATION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.OWNS_ATTRIBUTE_WAS_OVERRIDDEN;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.OWNS_KEY_PRECONDITION_OWNERSHIP_KEY_MISSING;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.OWNS_KEY_PRECONDITION_OWNERSHIP_KEY_TOO_MANY;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.OWNS_KEY_PRECONDITION_UNIQUENESS;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.OWNS_UNIQUE_PRECONDITION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.OWNS_VALUE_TYPE_NO_EXACT_EQUALITY;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.PLAYS_ABSTRACT_ROLE_TYPE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.PLAYS_ROLE_NOT_AVAILABLE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.ROOT_ATTRIBUTE_TYPE_CANNOT_BE_OWNED;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.ROOT_ROLE_TYPE_CANNOT_BE_PLAYED;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.ROOT_TYPE_MUTATION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.TYPE_HAS_INSTANCES_DELETE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.TYPE_HAS_INSTANCES_SET_ABSTRACT;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.TYPE_HAS_SUBTYPES;
import static com.vaticle.typedb.core.common.iterator.Iterators.compareSize;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.common.iterator.sorted.SortedIterators.Forwardable.emptySorted;
import static com.vaticle.typedb.core.common.iterator.sorted.SortedIterators.Forwardable.iterateSorted;
import static com.vaticle.typedb.core.common.parameters.Order.Asc.ASC;
import static com.vaticle.typedb.core.common.parameters.Concept.Transitivity.EXPLICIT;
import static com.vaticle.typedb.core.common.parameters.Concept.Transitivity.TRANSITIVE;
import static com.vaticle.typedb.core.encoding.Encoding.Edge.Type.OWNS;
import static com.vaticle.typedb.core.encoding.Encoding.Edge.Type.OWNS_KEY;
import static com.vaticle.typedb.core.encoding.Encoding.Edge.Type.PLAYS;
import static com.vaticle.typeql.lang.common.TypeQLToken.Annotation.KEY;
import static com.vaticle.typeql.lang.common.TypeQLToken.Annotation.UNIQUE;
import static com.vaticle.typeql.lang.common.TypeQLToken.Char.COMMA;
import static com.vaticle.typeql.lang.common.TypeQLToken.Char.SPACE;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Comparator.comparing;

public abstract class ThingTypeImpl extends TypeImpl implements ThingType {

    private final Cache cache;

    private static class Cache {
        private NavigableSet<Owns> owns = null;
        private NavigableSet<Owns> ownsExplicit = null;
    }

    ThingTypeImpl(ConceptManager conceptMgr, TypeVertex vertex) {
        super(conceptMgr, vertex);
        if (graphMgr().schema().isReadOnly()) cache = new Cache();
        else cache = null;
    }

    ThingTypeImpl(ConceptManager conceptMgr, String label, Encoding.Vertex.Type encoding) {
        super(conceptMgr, label, encoding);
        if (graphMgr().schema().isReadOnly()) this.cache = new Cache();
        else cache = null;
    }

    @Override
    public java.lang.String getSyntax() {
        StringBuilder builder = new StringBuilder();
        getSyntax(builder);
        return builder.toString();
    }

    @Override
    public void getSyntaxRecursive(StringBuilder builder) {
        getSyntax(builder);
        getSubtypes(EXPLICIT).stream()
                .sorted(comparing(x -> x.getLabel().name()))
                .forEach(x -> x.getSyntaxRecursive(builder));
    }

    protected void writeSupertype(StringBuilder builder) {
        if (getSupertype() != null) {
            builder.append(getLabel().name()).append(SPACE);
            builder.append(TypeQLToken.Constraint.SUB).append(SPACE);
            builder.append(getSupertype().getLabel().name());
        }
    }

    protected void writeAbstract(StringBuilder builder) {
        if (isAbstract()) builder.append(COMMA).append(SPACE).append(TypeQLToken.Constraint.ABSTRACT);
    }

    protected void writeOwns(StringBuilder builder) {
        Comparator<Owns> hasKey = (first, second) -> {
            boolean firstContainsKey = first.effectiveAnnotations().contains(KEY);
            boolean secondContainsKey = second.effectiveAnnotations().contains(KEY);
            if (firstContainsKey && !secondContainsKey) return -1;
            else if (secondContainsKey && !firstContainsKey) return 1;
            else return 0;
        };
        Comparator<Owns> hasUnique = (first, second) -> {
            boolean firstContainsUnique = first.effectiveAnnotations().contains(UNIQUE);
            boolean secondContainsUnique = second.effectiveAnnotations().contains(UNIQUE);
            if (firstContainsUnique && !secondContainsUnique) return -1;
            else if (secondContainsUnique && !firstContainsUnique) return 1;
            else return 0;
        };
        getOwns(EXPLICIT).stream().sorted(
                hasKey.thenComparing(hasUnique).thenComparing(owns -> owns.attributeType().getLabel().name())
        ).forEach(owns -> {
            builder.append(COMMA);
            owns.getSyntax(builder);
        });
    }

    protected void writePlays(StringBuilder builder) {
        getPlays(EXPLICIT).stream().sorted(comparing(x -> x.getLabel().scopedName())).forEach(roleType -> {
            builder.append(COMMA).append(TypeQLToken.Constraint.PLAYS).append(SPACE)
                    .append(roleType.getLabel().scopedName());
            RoleType overridden = getPlaysOverridden(roleType);
            if (overridden != null) {
                builder.append(SPACE).append(TypeQLToken.Constraint.AS).append(SPACE)
                        .append(overridden.getLabel().scopedName());
            }
        });
    }

    @Override
    public void setAbstract() {
        if (isAbstract()) return;
        validateIsNotDeleted();
        if (getInstances(EXPLICIT).first().isPresent()) {
            throw exception(TypeDBException.of(TYPE_HAS_INSTANCES_SET_ABSTRACT, getLabel()));
        }
        vertex.isAbstract(true);
    }

    @Override
    public void unsetAbstract() {
        if (isAbstract()) {
            validateIsNotDeleted();
            vertex.isAbstract(false);
        }
    }

    @Override
    public abstract ThingTypeImpl getSupertype();

    @Override
    public abstract Forwardable<? extends ThingTypeImpl, Order.Asc> getSupertypes();

    @Override
    public Forwardable<? extends ThingTypeImpl, Order.Asc> getSupertypesWithThing() {
        return iterateSorted(graphMgr().schema().getSupertypes(vertex), ASC)
                .mapSorted(v -> (ThingTypeImpl) conceptMgr.convertThingType(v), t -> t.vertex, ASC);
    }

    @Override
    public abstract Forwardable<? extends ThingTypeImpl, Order.Asc> getSubtypes();

    @Override
    public abstract Forwardable<? extends ThingTypeImpl, Order.Asc> getSubtypes(Transitivity transitivity);

    <THING extends ThingImpl> Forwardable<THING, Order.Asc> instances(Function<ThingVertex, THING> thingConstructor) {
        return instances(thingConstructor, TRANSITIVE);
    }

    <THING extends ThingImpl> Forwardable<THING, Order.Asc> instances(Function<ThingVertex, THING> thingConstructor, Transitivity transitivity) {
        Forwardable<ThingVertex, Order.Asc> instances;
        if (transitivity == EXPLICIT) instances = graphMgr().data().getReadable(vertex, ASC);
        else instances = getSubtypes().filter(t -> !t.isAbstract())
                .mergeMapForwardable(t -> graphMgr().data().getReadable(t.vertex, ASC), ASC);
        return instances.mapSorted(thingConstructor, ThingImpl::readableVertex, ASC);
    }

    @Override
    public void setOwns(AttributeType attributeType) {
        setOwns(attributeType, emptySet());
    }

    @Override
    public void setOwns(AttributeType attributeType, Set<TypeQLToken.Annotation> annotations) {
        setOwns(attributeType, null, annotations);
    }

    @Override
    public void setOwns(AttributeType attributeType, AttributeType overriddenType) {
        setOwns(attributeType, overriddenType, emptySet());
    }

    @Override
    public void setOwns(AttributeType attributeType, @Nullable AttributeType overriddenType, Set<TypeQLToken.Annotation> annotations) {
        validateIsNotDeleted();
        OwnsImpl.of(this, (AttributeTypeImpl) attributeType, overriddenType, annotations);
    }

    @Override
    public void unsetOwns(AttributeType attributeType) {
        validateIsNotDeleted();
        Optional<Owns> owns = getOwns(attributeType, EXPLICIT);
        if (owns.isPresent()) owns.get().delete();
        else if (getOwns(attributeType).isPresent()) {
            throw exception(TypeDBException.of(INVALID_UNDEFINE_INHERITED_OWNS, getLabel(), attributeType.getLabel()));
        } else {
            throw exception(TypeDBException.of(INVALID_UNDEFINE_NONEXISTENT_OWNS, getLabel(), attributeType.getLabel()));
        }
    }

    private static <T extends Type> void override(T owner, Encoding.Edge.Type encoding, T type, T overriddenType,
                                                  Forwardable<? extends Type, Order.Asc> overridable,
                                                  Forwardable<? extends Type, Order.Asc> notOverridable, // because the child can override the same type as parent but with annotations
                                                  ErrorMessage message) {
        if (type.getSupertypes().noneMatch(t -> t.equals(overriddenType))) {
            throw owner.exception(TypeDBException.of(message, owner.getLabel(), type.getLabel(), overriddenType.getLabel()));
        } else if (notOverridable.anyMatch(t -> t.equals(overriddenType)) || overridable.noneMatch(t -> t.equals(overriddenType))) {
            throw owner.exception(TypeDBException.of(OVERRIDE_NOT_AVAILABLE, type.getLabel(), overriddenType.getLabel()));
        }

        ((TypeImpl) owner).vertex.outs().edge(encoding, ((TypeImpl) type).vertex).setOverridden(((TypeImpl) overriddenType).vertex);
    }

    @Override
    public Optional<Owns> getOwns(AttributeType attributeType) {
        return getOwns(attributeType, TRANSITIVE);
    }

    @Override
    public Optional<Owns> getOwns(AttributeType attributeType, Transitivity transitivity) {
        // TODO: optimise for non-cached setting by using point lookups rather than a scan
        return iterate(getOwns(transitivity)).filter(owns -> owns.attributeType().equals(attributeType)).first();
    }

    @Override
    public NavigableSet<Owns> getOwns() {
        return getOwns(TRANSITIVE);
    }

    @Override
    public NavigableSet<Owns> getOwns(Transitivity transitivity) {
        // TODO: refactor cache
        if (transitivity == TRANSITIVE) {
            if (cache != null) {
                if (cache.owns == null) cache.owns = fetchOwns();
                return cache.owns;
            } else return fetchOwns();
        } else {
            if (cache != null) {
                if (cache.ownsExplicit == null) cache.ownsExplicit = fetchOwnsExplicit();
                return cache.ownsExplicit;
            } else return fetchOwnsExplicit();
        }
    }

    @Override
    public Forwardable<Owns, Order.Asc> getOwns(Set<TypeQLToken.Annotation> annotations) {
        return getOwns(annotations, TRANSITIVE);
    }

    @Override
    public Forwardable<Owns, Order.Asc> getOwns(Set<TypeQLToken.Annotation> annotations, Transitivity transitivity) {
        return iterateSorted(getOwns(transitivity), ASC).filter(owns -> owns.effectiveAnnotations().containsAll(annotations));
    }

    @Override
    public Forwardable<Owns, Order.Asc> getOwns(AttributeType.ValueType valueType) {
        return getOwns(valueType, TRANSITIVE);
    }

    @Override
    public Forwardable<Owns, Order.Asc> getOwns(AttributeType.ValueType valueType, Set<TypeQLToken.Annotation> annotations) {
        return getOwns(valueType, annotations, TRANSITIVE);
    }

    @Override
    public Forwardable<Owns, Order.Asc> getOwns(AttributeType.ValueType valueType, Transitivity transitivity) {
        return getOwns(valueType, emptySet(), transitivity);
    }

    @Override
    public Forwardable<Owns, Order.Asc> getOwns(AttributeType.ValueType valueType, Set<TypeQLToken.Annotation> annotations, Transitivity transitivity) {
        return getOwns(annotations, transitivity).filter(owns -> owns.attributeType().getValueType().equals(valueType));
    }

    private NavigableSet<Owns> fetchOwns() {
        NavigableSet<Owns> owns = new TreeSet<>();
        graphMgr().schema().ownedAttributeTypes(vertex, emptySet()).forEach(
                v -> owns.add(OwnsImpl.of(this, (AttributeTypeImpl) conceptMgr.convertAttributeType(v)))
        );
        return owns;
    }

    private NavigableSet<Owns> fetchOwnsExplicit() {
        if (isRoot()) return new TreeSet<>();
        NavigableSet<Owns> ownsExplicit = new TreeSet<>();
        vertex.outs().edge(OWNS_KEY).to().link(vertex.outs().edge(OWNS).to()).forEachRemaining(v ->
                ownsExplicit.add(OwnsImpl.of(this, (AttributeTypeImpl) conceptMgr.convertAttributeType(v)))
        );
        return ownsExplicit;
    }

    public AttributeType getOwnsOverridden(AttributeType attributeType) {
        TypeVertex attrVertex = graphMgr().schema().getType(attributeType.getLabel());
        if (attrVertex != null) {
            TypeEdge ownsEdge = vertex.outs().edge(OWNS_KEY, attrVertex);
            if (ownsEdge != null && ownsEdge.overridden().isPresent()) {
                return conceptMgr.convertAttributeType(ownsEdge.overridden().get());
            }
            ownsEdge = vertex.outs().edge(OWNS, attrVertex);
            if (ownsEdge != null && ownsEdge.overridden().isPresent()) {
                return conceptMgr.convertAttributeType(ownsEdge.overridden().get());
            }
        }
        return null;
    }

    @Override
    public void setPlays(RoleType roleType) {
        validateIsNotDeleted();
        if (roleType.isRoot()) {
            throw exception(TypeDBException.of(ROOT_ROLE_TYPE_CANNOT_BE_PLAYED));
        } else if (getSupertypes().filter(t -> !t.equals(this)).mergeMapForwardable(ThingType::getPlays, ASC).findFirst(roleType).isPresent()) {
            throw exception(TypeDBException.of(PLAYS_ROLE_NOT_AVAILABLE, getLabel(), roleType.getLabel()));
        }
        TypeEdge existingEdge = vertex.outs().edge(PLAYS, ((RoleTypeImpl) roleType).vertex);
        if (existingEdge != null) existingEdge.unsetOverridden();
        else vertex.outs().put(PLAYS, ((RoleTypeImpl) roleType).vertex);
    }

    @Override
    public void setPlays(RoleType roleType, RoleType overriddenType) {
        validateIsNotDeleted();
        setPlays(roleType);
        override(this, PLAYS, roleType, overriddenType, getSupertype().getPlays(),
                vertex.outs().edge(PLAYS).to().mapSorted(v -> (RoleTypeImpl) conceptMgr.convertRoleType(v), rt -> rt.vertex, ASC),
                OVERRIDDEN_PLAYED_ROLE_TYPE_NOT_SUPERTYPE);
    }

    @Override
    public void unsetPlays(RoleType roleType) {
        validateIsNotDeleted();
        TypeEdge edge = vertex.outs().edge(PLAYS, ((RoleTypeImpl) roleType).vertex);
        if (edge == null) {
            if (this.getPlays().findFirst(roleType).isPresent()) {
                throw exception(TypeDBException.of(INVALID_UNDEFINE_INHERITED_PLAYS,
                        this.getLabel().toString(), roleType.getLabel().toString()));
            } else {
                throw exception(TypeDBException.of(INVALID_UNDEFINE_NONEXISTENT_PLAYS,
                        this.getLabel().toString(), roleType.getLabel().toString()));
            }
        }
        if (getInstances().anyMatch(thing -> thing.getRelations(roleType).first().isPresent())) {
            throw exception(TypeDBException.of(INVALID_UNDEFINE_PLAYS_HAS_INSTANCES, vertex.label(), roleType.getLabel().toString()));
        }
        edge.delete();
    }

    @Override
    public Forwardable<RoleType, Order.Asc> getPlays() {
        return getPlays(TRANSITIVE);
    }
    
    @Override
    public Forwardable<RoleType, Order.Asc> getPlays(Transitivity transitivity) {
        if (isRoot()) return emptySorted();
        Forwardable<TypeVertex, Order.Asc> plays;
        if (transitivity == EXPLICIT) plays = vertex.outs().edge(PLAYS).to();
        else plays = iterateSorted(graphMgr().schema().playedRoleTypes(vertex), ASC);
        return plays.mapSorted(conceptMgr::convertRoleType, roleType -> ((RoleTypeImpl) roleType).vertex, ASC);
    }

    @Override
    public RoleType getPlaysOverridden(RoleType roleType) {
        TypeVertex roleVertex = graphMgr().schema().getType(roleType.getLabel());
        if (roleVertex != null) {
            TypeEdge playsEdge = vertex.outs().edge(PLAYS, roleVertex);
            if (playsEdge != null && playsEdge.overridden().isPresent()) {
                return conceptMgr.convertRoleType(playsEdge.overridden().get());
            }
        }
        return null;
    }

    @Override
    public void delete() {
        validateDelete();
        vertex.delete();
    }

    @Override
    void validateDelete() {
        super.validateDelete();
        if (getSubtypes().anyMatch(s -> !s.equals(this))) {
            throw exception(TypeDBException.of(TYPE_HAS_SUBTYPES, getLabel()));
        } else if (getSubtypes().flatMap(ThingType::getInstances).first().isPresent()) {
            throw exception(TypeDBException.of(TYPE_HAS_INSTANCES_DELETE, getLabel()));
        }
    }

    @Override
    public List<TypeDBException> exceptions() {
        return concatToList(
                validateIsAbstractOrOwnedAttributeTypesNotAbstract(),
                validateIsAbstractOrPlayedRoleTypesNotAbstract(),
                validateOverriddenOwnedAttributeTypesAreSupertypes(),
                validateOverriddenPlayedRoleTypesAreSupertypes()
        );
    }

    private List<TypeDBException> validateIsAbstractOrOwnedAttributeTypesNotAbstract() {
        if (isAbstract()) return emptyList();
        else return iterate(getOwns()).map(ThingType.Owns::attributeType).filter(Type::isAbstract).map(
                attType -> TypeDBException.of(OWNS_ABSTRACT_ATTRIBUTE_TYPE, getLabel(), attType.getLabel())
        ).toList();
    }

    private List<TypeDBException> validateIsAbstractOrPlayedRoleTypesNotAbstract() {
        if (isAbstract()) return emptyList();
        else return getPlays().filter(Type::isAbstract).map(
                roleType -> TypeDBException.of(PLAYS_ABSTRACT_ROLE_TYPE, getLabel(), roleType.getLabel())
        ).toList();
    }

    private List<TypeDBException> validateOverriddenOwnedAttributeTypesAreSupertypes() {
        return iterate(getOwns())
                .filter(owns -> owns.overridden().isPresent())
                .filter(owns -> owns.attributeType().getSupertypes().noneMatch(s -> s.equals(owns.overridden().get()))).map(
                        p -> TypeDBException.of(OVERRIDDEN_OWNED_ATTRIBUTE_TYPE_NOT_SUPERTYPE,
                                getLabel(), p.attributeType().getLabel(), p.overridden().get().getLabel())
                ).toList();
    }

    private List<TypeDBException> validateOverriddenPlayedRoleTypesAreSupertypes() {
        return getPlays().map(rt -> pair(rt, getPlaysOverridden(rt)))
                .filter(p -> p.second() != null)
                .filter(p -> p.first().getSupertypes().noneMatch(s -> s.equals(p.second()))).map(
                        p -> TypeDBException.of(OVERRIDDEN_PLAYED_ROLE_TYPE_NOT_SUPERTYPE,
                                getLabel(), p.first().getLabel(), p.second().getLabel())
                ).toList();
    }

    @Override
    public boolean isThingType() {
        return true;
    }

    @Override
    public ThingTypeImpl asThingType() {
        return this;
    }

    public static class Root extends ThingTypeImpl {

        public Root(ConceptManager conceptMgr, TypeVertex vertex) {
            super(conceptMgr, vertex);
            assert vertex.label().equals(Encoding.Vertex.Type.Root.THING.label());
        }

        @Override
        public boolean isRoot() {
            return true;
        }

        @Override
        public void delete() {
            throw exception(TypeDBException.of(ROOT_TYPE_MUTATION));
        }

        @Override
        public void setLabel(String label) {
            throw exception(TypeDBException.of(ROOT_TYPE_MUTATION));
        }

        @Override
        public void unsetAbstract() {
            throw exception(TypeDBException.of(ROOT_TYPE_MUTATION));
        }

        @Override
        public ThingTypeImpl getSupertype() {
            return null;
        }

        @Override
        public Forwardable<ThingTypeImpl, Order.Asc> getSupertypes() {
            return iterateSorted(ASC, this);
        }

        @Override
        public Forwardable<ThingTypeImpl, Order.Asc> getSubtypes() {
            return getSubtypes(TRANSITIVE);
        }

        @Override
        public Forwardable<ThingTypeImpl, Order.Asc> getSubtypes(Transitivity transitivity) {
            if (transitivity == EXPLICIT) return getSubtypes(EXPLICIT, v -> {
                switch (v.encoding()) {
                    case ENTITY_TYPE:
                        return (ThingTypeImpl) conceptMgr.convertEntityType(v);
                    case ATTRIBUTE_TYPE:
                        return (ThingTypeImpl) conceptMgr.convertAttributeType(v);
                    case RELATION_TYPE:
                        return (ThingTypeImpl) conceptMgr.convertRelationType(v);
                    default:
                        throw exception(TypeDBException.of(UNRECOGNISED_VALUE));
                }
            });
            else return getSubtypes(TRANSITIVE, v -> {
                switch (v.encoding()) {
                    case THING_TYPE:
                        assert vertex == v;
                        return this;
                    case ENTITY_TYPE:
                        return (ThingTypeImpl) conceptMgr.convertEntityType(v);
                    case ATTRIBUTE_TYPE:
                        return (ThingTypeImpl) conceptMgr.convertAttributeType(v);
                    case RELATION_TYPE:
                        return (ThingTypeImpl) conceptMgr.convertRelationType(v);
                    default:
                        throw exception(TypeDBException.of(UNRECOGNISED_VALUE));
                }
            });
        }

        @Override
        public Forwardable<ThingImpl, Order.Asc> getInstances() {
            return getInstances(TRANSITIVE);
        }

        @Override
        public Forwardable<ThingImpl, Order.Asc> getInstances(Transitivity transitivity) {
            if (transitivity == EXPLICIT)
                return emptySorted();
            else
                return instances(v -> {
                    switch (v.encoding()) {
                        case ENTITY:
                            return EntityImpl.of(conceptMgr, v);
                        case ATTRIBUTE:
                            return AttributeImpl.of(conceptMgr, v);
                        case RELATION:
                            return RelationImpl.of(conceptMgr, v);
                        default:
                            assert false;
                            throw exception(TypeDBException.of(UNRECOGNISED_VALUE));
                    }
                });
        }

        @Override
        public void setOwns(AttributeType attributeType, Set<TypeQLToken.Annotation> annotations) {
            throw exception(TypeDBException.of(ROOT_TYPE_MUTATION));
        }

        @Override
        public void setOwns(AttributeType attributeType, AttributeType overriddenType, Set<TypeQLToken.Annotation> annotations) {
            throw exception(TypeDBException.of(ROOT_TYPE_MUTATION));
        }

        @Override
        public void setPlays(RoleType roleType) {
            throw exception(TypeDBException.of(ROOT_TYPE_MUTATION));
        }

        @Override
        public void setPlays(RoleType roleType, RoleType overriddenType) {
            throw exception(TypeDBException.of(ROOT_TYPE_MUTATION));
        }

        @Override
        public void unsetPlays(RoleType roleType) {
            throw exception(TypeDBException.of(ROOT_TYPE_MUTATION));
        }

        @Override
        public void getSyntax(StringBuilder builder) {
            builder.append(Encoding.Vertex.Type.Root.THING.label());
        }

        /**
         * No-op validation method of the root type 'thing'.
         * <p>
         * There's nothing to validate for the root type 'thing'.
         */
        @Override
        public List<TypeDBException> exceptions() {
            return emptyList();
        }
    }

    private static class OwnsImpl implements Owns {

        private final ThingTypeImpl owner;
        private final AttributeTypeImpl attributeType;
        private TypeEdge edge;
        private AttributeType overridden;
        private Set<TypeQLToken.Annotation> effectiveAnnotationsCache;

        private OwnsImpl(ThingTypeImpl owner, AttributeTypeImpl attributeType, TypeEdge edge) {
            this.owner = owner;
            this.attributeType = attributeType;
            this.edge = edge;
            this.overridden = edge.overridden().map(owner.conceptMgr::convertAttributeType).orElse(null);
        }

        private static OwnsImpl of(ThingTypeImpl owner, AttributeTypeImpl attributeType) {
            TypeVertex attVertex = attributeType.vertex;
            TypeEdge edge = null;
            ThingTypeImpl sup = owner;
            while (sup.getSupertype() != null && edge == null) {
                if ((edge = sup.vertex.outs().edge(OWNS, attVertex)) == null &&
                        (edge = sup.vertex.outs().edge(OWNS_KEY, attVertex)) == null) {
                    sup = sup.getSupertype();
                }
            }
            if (edge == null) throw TypeDBException.of(ILLEGAL_STATE);
            return new OwnsImpl(owner, attributeType, edge);
        }

        private static OwnsImpl of(ThingTypeImpl owner, AttributeTypeImpl attributeType,
                                   @Nullable AttributeType overriddenType, Set<TypeQLToken.Annotation> annotations) {
            validateSchema(owner, attributeType, overriddenType, annotations);

            Optional<Owns> existingOrInherited = iterate(owner.getOwns()).filter(owns -> owns.attributeType().equals(attributeType)).first();
            validateData(owner, attributeType, annotations, existingOrInherited.orElse(null));

            Optional<Owns> existingExplicit = iterate(owner.getOwns(EXPLICIT))
                    .filter(ownsExplicit -> ownsExplicit.attributeType().equals(attributeType)).first();
            if (existingExplicit.isPresent()) {
                OwnsImpl existingOwnsImpl = ((OwnsImpl) existingExplicit.get());
                existingOwnsImpl.edge.delete();
                existingOwnsImpl.edge = createEdge(owner, attributeType, overriddenType, annotations);
                existingOwnsImpl.overridden = overriddenType;
                return existingOwnsImpl;
            } else {
                TypeEdge edge = createEdge(owner, attributeType, overriddenType, annotations);
                return new OwnsImpl(owner, attributeType, edge);
            }
        }

        private static TypeEdge createEdge(
                ThingTypeImpl owner, AttributeTypeImpl attributeType, AttributeType overriddenType,
                Set<TypeQLToken.Annotation> annotations
        ) {
            TypeVertex ownerVertex = owner.vertex;
            TypeVertex attVertex = attributeType.vertex;
            TypeEdge edge;
            // TODO: once we have one type of owns edge, update the ownership edge rather than deleting & creating it
            if (annotations.contains(KEY)) edge = ownerVertex.outs().put(OWNS_KEY, attVertex);
            else edge = ownerVertex.outs().put(OWNS, attVertex);

            if (overriddenType == null) {
                Optional<Owns> parentOwns = iterate(owner.getSupertype().getOwns())
                        .filter(owns -> owns.attributeType().equals(attributeType)).first();
                if (parentOwns.isPresent()) edge.setOverridden(attVertex);
            } else edge.setOverridden(((TypeImpl) overriddenType).vertex);
            edge.setAnnotations(annotations);
            return edge;
        }

        private static void validateSchema(
                ThingTypeImpl owner, AttributeTypeImpl attributeType, @Nullable AttributeType overriddenType,
                Set<TypeQLToken.Annotation> annotations
        ) {
            if (attributeType.isRoot()) {
                throw owner.exception(TypeDBException.of(ROOT_ATTRIBUTE_TYPE_CANNOT_BE_OWNED));
            } else if (annotations.contains(KEY) && annotations.contains(UNIQUE)) {
                throw owner.exception(TypeDBException.of(OWNS_ANNOTATION_DECLARATION_INCOMPATIBLE, owner.getLabel(), attributeType.getLabel(), KEY, UNIQUE));
            }

            Set<Owns> superOwns = owner.getSupertype().getOwns();
            if (overriddenType != null) {
                if (attributeType.getSupertypes().noneMatch(overriddenType::equals)) {
                    throw owner.exception(TypeDBException.of(
                            OVERRIDDEN_OWNED_ATTRIBUTE_TYPE_NOT_SUPERTYPE, owner.getLabel(), attributeType.getLabel(),
                            overriddenType.getLabel()
                    ));
                }
                Optional<Owns> parentOwns = iterate(superOwns).filter(owns -> owns.attributeType().equals(overriddenType)).first();
                if (parentOwns.isEmpty()) {
                    throw owner.exception(TypeDBException.of(OVERRIDE_NOT_AVAILABLE, owner.getLabel(), overriddenType.getLabel()));
                } else {
                    validateAgainstParent(parentOwns.get(), owner, attributeType, annotations);
                }
            } else {
                Optional<Owns> parentOwns = iterate(superOwns).filter(owns -> owns.attributeType().equals(attributeType)).first();
                parentOwns.ifPresent(owns -> {
                    if (annotations.isEmpty())
                        throw TypeDBException.of(OWNS_ATTRIBUTE_REDECLARATION, owner.getLabel(), attributeType.getLabel());
                    validateAgainstParent(owns, owner, attributeType, annotations);
                });
            }

            FunctionalIterator<AttributeType> hiddenByOverride = owner.getSupertypes().flatMap(t -> iterate(t.getOwns()))
                    .filter(owns -> owns.overridden().map(t -> !t.equals(owns.attributeType())).orElse(false))
                    .map(owns -> owns.overridden().get());
            if (hiddenByOverride.filter(t -> t.equals(attributeType)).first().isPresent()) {
                throw owner.exception(TypeDBException.of(OWNS_ATTRIBUTE_WAS_OVERRIDDEN, owner.getLabel(), attributeType.getLabel()));
            }

            if ((annotations.contains(KEY) || annotations.contains(UNIQUE)) && !attributeType.getValueType().hasExactEquality()) {
                throw owner.exception(TypeDBException.of(OWNS_VALUE_TYPE_NO_EXACT_EQUALITY, owner.getLabel(), attributeType.getLabel(), annotations, attributeType.getValueType().name()));
            }
        }

        private static void validateAgainstParent(
                Owns parent, Type owner, AttributeType attribute, Set<TypeQLToken.Annotation> annotations
        ) {
            Set<TypeQLToken.Annotation> inheritedAnnotations = parent.effectiveAnnotations();
            boolean inheritedKey = inheritedAnnotations.contains(KEY);
            boolean childUnique = annotations.contains(UNIQUE);
            if (inheritedKey && childUnique) {
                throw owner.exception(TypeDBException.of(OWNS_ANNOTATION_LESS_STRICT_THAN_PARENT, owner.getLabel(), attribute.getLabel(), annotations, parent.toString()));
            }
            Set<TypeQLToken.Annotation> redeclared = new HashSet<>(annotations);
            redeclared.retainAll(inheritedAnnotations);
            if (!redeclared.isEmpty()) {
                throw owner.exception(TypeDBException.of(OWNS_ANNOTATION_REDECLARATION, owner.getLabel(), attribute.getLabel(), redeclared));
            }
        }

        private static void validateData(ThingTypeImpl owner, AttributeTypeImpl attributeType,
                                         Set<TypeQLToken.Annotation> annotations, @Nullable Owns existingOwns) {
            if (annotations.contains(KEY)) {
                if (existingOwns == null || existingOwns.effectiveAnnotations().isEmpty()) {
                    owner.getInstances().forEachRemaining(instance -> validateDataKey(owner, instance, attributeType));
                } else if (existingOwns.effectiveAnnotations().contains(UNIQUE)) {
                    owner.getInstances().forEachRemaining(instance -> validateDataKeyCardinality(owner, instance, attributeType));
                } else {
                    assert existingOwns.effectiveAnnotations().contains(KEY);
                }
            } else if (annotations.contains(UNIQUE)) {
                if (existingOwns == null || existingOwns.effectiveAnnotations().isEmpty()) {
                    owner.getInstances().forEachRemaining(instance -> validateDataUnique(owner, instance, attributeType));
                } else {
                    assert existingOwns.effectiveAnnotations().contains(KEY) || existingOwns.effectiveAnnotations().contains(UNIQUE);
                }
            }
        }

        private static void validateDataKey(ThingType ownerType, Thing owner, AttributeType attributeType) {
            FunctionalIterator<? extends Attribute> attrs = owner.getHas(attributeType);
            if (!attrs.hasNext()) {
                throw ownerType.exception(TypeDBException.of(OWNS_KEY_PRECONDITION_OWNERSHIP_KEY_TOO_MANY, ownerType.getLabel(), attributeType.getLabel()));
            }
            Attribute key = attrs.next();
            if (attrs.hasNext()) {
                throw ownerType.exception(TypeDBException.of(OWNS_KEY_PRECONDITION_OWNERSHIP_KEY_MISSING, ownerType.getLabel(), attributeType.getLabel()));
            } else if (compareSize(key.getOwners(ownerType), 1) != 0) {
                throw ownerType.exception(TypeDBException.of(OWNS_KEY_PRECONDITION_UNIQUENESS, attributeType.getLabel(), ownerType.getLabel()));
            }
        }

        private static void validateDataKeyCardinality(ThingTypeImpl ownerType, Thing owner, AttributeTypeImpl attributeType) {
            FunctionalIterator<? extends Attribute> attrs = owner.getHas(attributeType);
            if (!attrs.hasNext()) {
                throw ownerType.exception(TypeDBException.of(OWNS_KEY_PRECONDITION_OWNERSHIP_KEY_TOO_MANY, ownerType.getLabel(), attributeType.getLabel()));
            }
            Attribute key = attrs.next();
            if (attrs.hasNext()) {
                throw ownerType.exception(TypeDBException.of(OWNS_KEY_PRECONDITION_OWNERSHIP_KEY_MISSING, ownerType.getLabel(), attributeType.getLabel()));
            }
        }

        private static void validateDataUnique(ThingTypeImpl ownerType, Thing instance, AttributeTypeImpl attributeType) {
            instance.getHas(attributeType).forEachRemaining(attr -> {
                if (compareSize(attr.getOwners(ownerType), 1) != 0) {
                    throw ownerType.exception(TypeDBException.of(OWNS_UNIQUE_PRECONDITION, attributeType.getLabel(), ownerType.getLabel()));
                }
            });
        }

        @Override
        public AttributeType attributeType() {
            return attributeType;
        }

        @Override
        public Set<TypeQLToken.Annotation> effectiveAnnotations() {
            if (owner.graphMgr().schema().isReadOnly()) {
                if (effectiveAnnotationsCache == null) effectiveAnnotationsCache = computeEffectiveAnnotations();
                return effectiveAnnotationsCache;
            } else return computeEffectiveAnnotations();
        }

        private Set<TypeQLToken.Annotation> computeEffectiveAnnotations() {
            Set<TypeQLToken.Annotation> annotations = new HashSet<>(explicitAnnotations());
            owner.getSupertype().getOwns(attributeType)
                    .or(() -> overridden().flatMap(overridden -> owner.getSupertype().getOwns(overridden)))
                    .ifPresent(owns -> annotations.addAll(owns.effectiveAnnotations()));
            // in the future, we will collapse parametrised annotations of the same type to the strictest (child) annotation
            // for example, @card(0,10) from the parent should not be kept if @card(1,5) is defined on the child
            return annotations;
        }

        private Set<TypeQLToken.Annotation> explicitAnnotations() {
            return edge.annotations();
        }

        @Override
        public Optional<AttributeType> overridden() {
            return Optional.ofNullable(overridden);
        }

        @Override
        public void delete() {
            if (!edge.isDeleted()) {
                if (owner.getInstances().anyMatch(thing -> thing.getHas(attributeType).first().isPresent())) {
                    throw owner.exception(TypeDBException.of(INVALID_UNDEFINE_OWNS_HAS_INSTANCES, owner.getLabel(), attributeType.getLabel()));
                }
                this.edge.delete();
            }
        }

        @Override
        public int compareTo(Owns o) {
            return attributeType.compareTo(o.attributeType());
        }

        @Override
        public void getSyntax(StringBuilder builder) {
            builder.append(TypeQLToken.Constraint.OWNS).append(SPACE)
                    .append(attributeType().getLabel().name());
            overridden().ifPresent(ownsOverridden ->
                    builder.append(SPACE).append(TypeQLToken.Constraint.AS).append(SPACE)
                            .append(ownsOverridden.getLabel().name())
            );
            explicitAnnotations().stream().sorted(Comparator.comparing(TypeQLToken.Annotation::toString))
                    .forEach(annotation -> builder.append(SPACE).append(annotation));
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append(owner.getLabel()).append(SPACE);
            getSyntax(builder);
            return builder.toString();
        }
    }
}
