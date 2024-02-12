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
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Forwardable;
import com.vaticle.typedb.core.common.parameters.Concept.Transitivity;
import com.vaticle.typedb.core.common.parameters.Order;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.thing.impl.AttributeImpl;
import com.vaticle.typedb.core.concept.thing.impl.EntityImpl;
import com.vaticle.typedb.core.concept.thing.impl.RelationImpl;
import com.vaticle.typedb.core.concept.thing.impl.ThingImpl;
import com.vaticle.typedb.core.concept.type.AttributeType;
import com.vaticle.typedb.core.concept.type.RoleType;
import com.vaticle.typedb.core.concept.type.ThingType;
import com.vaticle.typedb.core.concept.type.Type;
import com.vaticle.typedb.core.concept.validation.DeclarationValidation;
import com.vaticle.typedb.core.concept.validation.SubtypeValidation;
import com.vaticle.typedb.core.encoding.Encoding;
import com.vaticle.typedb.core.graph.edge.TypeEdge;
import com.vaticle.typedb.core.graph.vertex.ThingVertex;
import com.vaticle.typedb.core.graph.vertex.TypeVertex;
import com.vaticle.typeql.lang.common.TypeQLToken;
import com.vaticle.typeql.lang.common.TypeQLToken.Annotation;

import javax.annotation.Nullable;
import java.util.ArrayList;
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
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.OVERRIDDEN_OWNED_ATTRIBUTE_NOT_AVAILABLE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.OVERRIDDEN_OWNED_ATTRIBUTE_TYPE_NOT_SUPERTYPE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.OVERRIDDEN_PLAYED_ROLE_NOT_AVAILABLE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.OVERRIDDEN_PLAYED_ROLE_TYPE_NOT_SUPERTYPE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.OWNS_ABSTRACT_ATTRIBUTE_TYPE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.OWNS_ANNOTATION_DECLARATION_INCOMPATIBLE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.OWNS_ANNOTATION_LESS_STRICT_THAN_PARENT;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.OWNS_ATTRIBUTE_WAS_OVERRIDDEN;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.OWNS_OVERRIDE_ANNOTATIONS_REDUNDANT;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.OWNS_VALUE_TYPE_NO_EXACT_EQUALITY;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.PLAYS_ABSTRACT_ROLE_TYPE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.PLAYS_ROLE_NOT_AVAILABLE_OVERRIDDEN;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.REDUNDANT_OWNS_DECLARATION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.REDUNDANT_PLAYS_DECLARATION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.ROOT_ATTRIBUTE_TYPE_CANNOT_BE_OWNED;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.ROOT_ROLE_TYPE_CANNOT_BE_PLAYED;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.ROOT_TYPE_MUTATION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.SCHEMA_VALIDATION_INVALID_DEFINE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.SCHEMA_VALIDATION_INVALID_UNDEFINE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.TYPE_HAS_INSTANCES_DELETE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.TYPE_HAS_INSTANCES_SET_ABSTRACT;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.TYPE_HAS_SUBTYPES;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.common.iterator.sorted.SortedIterators.Forwardable.emptySorted;
import static com.vaticle.typedb.core.common.iterator.sorted.SortedIterators.Forwardable.iterateSorted;
import static com.vaticle.typedb.core.common.parameters.Concept.Transitivity.EXPLICIT;
import static com.vaticle.typedb.core.common.parameters.Concept.Transitivity.TRANSITIVE;
import static com.vaticle.typedb.core.common.parameters.Order.Asc.ASC;
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

        private Set<AttributeType> ownedAttributes = null;

        private Set<AttributeType> ownedAttributesExplicit = null;

        NavigableSet<Owns> getOwns(Transitivity transitivity) {
            if (transitivity == EXPLICIT) return ownsExplicit;
            else return owns;
        }

        void putOwns(Transitivity transitivity, NavigableSet<Owns> value) {
            if (transitivity == EXPLICIT) ownsExplicit = value;
            else owns = value;
        }

        public Set<AttributeType> getOwnedAttributes(Transitivity transitivity) {
            if (transitivity == EXPLICIT) return ownedAttributesExplicit;
            else return ownedAttributes;
        }

        public void putOwnedAttributes(Transitivity transitivity, Set<AttributeType> attributes) {
            if (transitivity == EXPLICIT) ownedAttributesExplicit = attributes;
            else ownedAttributes = attributes;
        }
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
                        .append(overridden.getLabel().name());
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
        return instances(TRANSITIVE, thingConstructor);
    }

    <THING extends ThingImpl> Forwardable<THING, Order.Asc> instances(Transitivity transitivity, Function<ThingVertex, THING> thingConstructor) {
        Forwardable<ThingVertex, Order.Asc> instances;
        if (transitivity == EXPLICIT) instances = graphMgr().data().getReadable(vertex, ASC);
        else {
            instances = getSubtypes().filter(t -> !t.isAbstract())
                    .mergeMapForwardable(t -> graphMgr().data().getReadable(t.vertex, ASC), ASC);
        }
        return instances.mapSorted(thingConstructor, ThingImpl::readableVertex, ASC);
    }

    @Override
    public void setOwns(AttributeType attributeType) {
        setOwns(attributeType, emptySet());
    }

    @Override
    public void setOwns(AttributeType attributeType, Set<Annotation> annotations) {
        setOwns(attributeType, null, annotations);
    }

    @Override
    public void setOwns(AttributeType attributeType, AttributeType overriddenType) {
        setOwns(attributeType, overriddenType, emptySet());
    }

    @Override
    public void setOwns(AttributeType attributeType, @Nullable AttributeType overriddenType, Set<Annotation> annotations) {
        validateIsNotDeleted();
        if (attributeType.isRoot()) {
            throw exception(TypeDBException.of(ROOT_ATTRIBUTE_TYPE_CANNOT_BE_OWNED));
        }
        DeclarationValidation.Owns.validateAdd(this, attributeType, annotations).forEach(e -> { throw exception(e); });
        if (overriddenType != null) {
            DeclarationValidation.Owns.validateOverride(this, attributeType, overriddenType, annotations).forEach(e -> { throw exception(e); });
        }
        SubtypeValidation.throwIfNonEmpty(SubtypeValidation.Owns.validateAdd(this, attributeType, overriddenType, annotations), e ->
                exception(TypeDBException.of(SCHEMA_VALIDATION_INVALID_DEFINE, SubtypeValidation.Owns.format(this, attributeType, overriddenType, annotations), e))
        );
        OwnsImpl.of(this, (AttributeTypeImpl) attributeType, overriddenType, annotations);
    }

    @Override
    public void unsetOwns(AttributeType attributeType) {
        validateIsNotDeleted();
        Optional<Owns> owns = getOwns(EXPLICIT, attributeType);
        if (owns.isPresent()) {
            SubtypeValidation.throwIfNonEmpty(SubtypeValidation.Owns.validateRemove(this, attributeType),
                    errList -> exception(TypeDBException.of(SCHEMA_VALIDATION_INVALID_DEFINE, SubtypeValidation.Owns.format(this, attributeType, owns.get().overridden().orElse(null), owns.get().effectiveAnnotations()), errList)));
            ((OwnsImpl) owns.get()).delete();
        } else if (getOwns(attributeType).isPresent()) {
            throw exception(TypeDBException.of(INVALID_UNDEFINE_INHERITED_OWNS, getLabel(), attributeType.getLabel()));
        } else {
            throw exception(TypeDBException.of(INVALID_UNDEFINE_NONEXISTENT_OWNS, getLabel(), attributeType.getLabel()));
        }
    }

    @Override
    public Optional<Owns> getOwns(AttributeType attributeType) {
        return getOwns(TRANSITIVE, attributeType);
    }

    @Override
    public Optional<Owns> getOwns(Transitivity transitivity, AttributeType attributeType) {
        // TODO: optimise for non-cached setting by using point lookups rather than a scan
        return iterate(getOwns(transitivity)).filter(owns -> owns.attributeType().equals(attributeType)).first();
    }

    @Override
    public NavigableSet<Owns> getOwns() {
        return getOwns(TRANSITIVE);
    }

    @Override
    public NavigableSet<Owns> getOwns(Transitivity transitivity) {
        if (cache != null) {
            if (cache.getOwns(transitivity) == null) cache.putOwns(transitivity, fetchOwns(transitivity));
            return cache.getOwns(transitivity);
        } else return fetchOwns(transitivity);
    }

    @Override
    public Forwardable<Owns, Order.Asc> getOwns(Set<Annotation> annotations) {
        return getOwns(TRANSITIVE, annotations);
    }

    @Override
    public Forwardable<Owns, Order.Asc> getOwns(Transitivity transitivity, Set<Annotation> annotations) {
        return iterateSorted(getOwns(transitivity), ASC).filter(owns -> owns.effectiveAnnotations().containsAll(annotations));
    }

    @Override
    public Forwardable<Owns, Order.Asc> getOwns(AttributeType.ValueType valueType) {
        return getOwns(TRANSITIVE, valueType);
    }

    @Override
    public Forwardable<Owns, Order.Asc> getOwns(AttributeType.ValueType valueType, Set<Annotation> annotations) {
        return getOwns(TRANSITIVE, valueType, annotations);
    }

    @Override
    public Forwardable<Owns, Order.Asc> getOwns(Transitivity transitivity, AttributeType.ValueType valueType) {
        return getOwns(transitivity, valueType, emptySet());
    }

    @Override
    public Forwardable<Owns, Order.Asc> getOwns(Transitivity transitivity, AttributeType.ValueType valueType, Set<Annotation> annotations) {
        return getOwns(transitivity, annotations).filter(owns -> owns.attributeType().getValueType().equals(valueType));
    }

    private NavigableSet<Owns> fetchOwns(Transitivity transitivity) {
        return fetchOwnsVertices(transitivity)
                .map(v -> OwnsImpl.of(this, (AttributeTypeImpl) conceptMgr.convertAttributeType(v)))
                .collect(TreeSet::new);
    }

    private FunctionalIterator<TypeVertex> fetchOwnsVertices(Transitivity transitivity) {
        if (transitivity == EXPLICIT) return vertex.outs().edge(OWNS_KEY).to().link(vertex.outs().edge(OWNS).to());
        else return iterate(graphMgr().schema().ownedAttributeTypes(vertex, emptySet()));
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
    public Set<AttributeType> getOwnedAttributes(Transitivity transitivity) {
        if (cache != null) {
            if (cache.getOwnedAttributes(transitivity) == null) {
                cache.putOwnedAttributes(transitivity, iterate(getOwns(transitivity)).map(Owns::attributeType).toSet());
            }
            return cache.getOwnedAttributes(transitivity);
        } else {
            return iterate(getOwns(transitivity)).map(Owns::attributeType).toSet();
        }
    }

    @Override
    public void setPlays(RoleType roleType) {
        validateIsNotDeleted();
        DeclarationValidation.Plays.validateAdd(this, roleType).forEach(e -> { throw exception(e); });
        TypeEdge existingEdge = vertex.outs().edge(PLAYS, ((RoleTypeImpl) roleType).vertex);
        if (existingEdge != null) existingEdge.unsetOverridden();
        else vertex.outs().put(PLAYS, ((RoleTypeImpl) roleType).vertex);
    }

    @Override
    public void setPlays(RoleType roleType, RoleType overriddenType) {
        validateIsNotDeleted();
        DeclarationValidation.Plays.validateAdd(this, roleType).forEach(e -> { throw exception(e); });
        DeclarationValidation.Plays.validateOverride(this, roleType, overriddenType).forEach(e -> { throw exception(e); });
        SubtypeValidation.throwIfNonEmpty(SubtypeValidation.Plays.validateOverride(this, overriddenType), errList ->
                exception(TypeDBException.of(SCHEMA_VALIDATION_INVALID_DEFINE, SubtypeValidation.Plays.format(this, roleType, overriddenType), errList))
        );
        setPlays(roleType);
        vertex.outs().edge(PLAYS, ((RoleTypeImpl)roleType).vertex).setOverridden(((RoleTypeImpl) overriddenType).vertex);
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
        SubtypeValidation.throwIfNonEmpty(SubtypeValidation.Plays.validateRemove(this, roleType), e ->
                exception(TypeDBException.of(SCHEMA_VALIDATION_INVALID_UNDEFINE, SubtypeValidation.Plays.format(this, roleType, this.getPlaysOverridden(roleType)), e))
        );
        edge.delete();
    }

    @Override
    public boolean plays(RoleType roleType) {
        if (isRoot()) return false;
        assert getSupertype() != null;
        return graphMgr().schema().playedRoleTypes(vertex).contains(((RoleTypeImpl) roleType).vertex);
    }

    @Override
    public Forwardable<RoleType, Order.Asc> getPlays() {
        return getPlays(TRANSITIVE);
    }

    @Override
    public Forwardable<RoleType, Order.Asc> getPlays(Transitivity transitivity) {
        if (isRoot()) return emptySorted();
        return getPlaysVertices(transitivity).mapSorted(conceptMgr::convertRoleType, roleType -> ((RoleTypeImpl) roleType).vertex, ASC);
    }

    Forwardable<TypeVertex, Order.Asc> getPlaysVertices(Transitivity transitivity) {
        if (transitivity == EXPLICIT) return vertex.outs().edge(PLAYS).to();
        else return iterateSorted(graphMgr().schema().playedRoleTypes(vertex), ASC);
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
                validateOverriddenOwnedAttributeTypesAreSupertypesAndInherited(),
                validateOverriddenPlayedRoleTypesAreSuperTypesAndInherited(),
                validatePlaysAreNotRedeclared(),
                validateOwnsAreNotRedeclared()
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

    private List<TypeDBException> validateOverriddenOwnedAttributeTypesAreSupertypesAndInherited() {
        List<TypeDBException> exceptions = new ArrayList<>();
        iterate(getOwns(EXPLICIT))
                .filter(owns -> owns.overridden().isPresent())
                .forEachRemaining(owns -> {
                    if (owns.attributeType().getSupertypes().noneMatch(s -> s.equals(owns.overridden().get()))) {
                        exceptions.add(TypeDBException.of(OVERRIDDEN_OWNED_ATTRIBUTE_TYPE_NOT_SUPERTYPE,
                                getLabel(), owns.attributeType().getLabel(), owns.overridden().get().getLabel()));
                    }
                    if (!getSupertype().getOwnedAttributes(TRANSITIVE).contains(owns.overridden().get())) {
                        if (!owns.overridden().get().equals(owns.attributeType())) {
                            exceptions.add(TypeDBException.of(OVERRIDDEN_OWNED_ATTRIBUTE_NOT_AVAILABLE,
                                    getLabel(), owns.overridden().get().getLabel(), owns.attributeType().getLabel(), owns.overridden().get().getLabel()));
                        } // ignore self overrides, since these are not explicitly set by the user
                    }
                });
        return exceptions;
    }

    private List<TypeDBException> validateOverriddenPlayedRoleTypesAreSuperTypesAndInherited() {
        List<TypeDBException> exceptions = new ArrayList<>();
        getPlays().map(rt -> pair(rt, getPlaysOverridden(rt)))
                .filter(p -> p.second() != null)
                .forEachRemaining(p -> {
                    if (p.first().getSupertypes().noneMatch(s -> s.equals(p.second()))) {
                        exceptions.add(TypeDBException.of(OVERRIDDEN_PLAYED_ROLE_TYPE_NOT_SUPERTYPE,
                                getLabel(), p.first().getLabel(), p.second().getLabel()));
                    }
                    if (!getSupertype().plays(p.second())) {
                        exceptions.add(TypeDBException.of(OVERRIDDEN_PLAYED_ROLE_NOT_AVAILABLE,
                                getLabel(), p.first().getLabel(), p.second().getLabel(), p.second().getLabel()));
                    }
                });
        return exceptions;
    }

    private List<TypeDBException> validatePlaysAreNotRedeclared() {
        return getPlays(EXPLICIT).intersect(getSupertype().getPlays(TRANSITIVE))
                .map(roleType -> TypeDBException.of(REDUNDANT_PLAYS_DECLARATION, getLabel(), roleType.getLabel()))
                .toList();
    }

    private List<TypeDBException> validateOwnsAreNotRedeclared() {
        Set<AttributeType> redeclaredOwns = new HashSet<>(getOwnedAttributes(EXPLICIT));
        redeclaredOwns.retainAll(getSupertype().getOwnedAttributes(TRANSITIVE));
        FunctionalIterator<TypeDBException> redundantRedeclarations = iterate(redeclaredOwns)
                .filter(attributeType -> {
                    return !OwnsImpl.isFirstStricter(
                            getOwns(EXPLICIT, attributeType).get().effectiveAnnotations(),
                            getSupertype().getOwns(TRANSITIVE, attributeType).get().effectiveAnnotations());
                }).map(attributeType -> TypeDBException.of(REDUNDANT_OWNS_DECLARATION, getLabel(), attributeType.getLabel()));

        FunctionalIterator<TypeDBException> overridesWithRedundantAnnotations = Iterators.iterate(getOwns(EXPLICIT))
                .filter(owns -> owns.overridden().isPresent() && getSupertype().getOwns(TRANSITIVE, owns.overridden().get()).isPresent())
                .filter(owns -> {
                    return !((OwnsImpl) owns).explicitAnnotations().isEmpty() && !OwnsImpl.isFirstStricter(
                                    ((OwnsImpl) owns).explicitAnnotations(),
                                    getSupertype().getOwns(TRANSITIVE, owns.overridden().get()).get().effectiveAnnotations()
                            );
                }).map(owns -> TypeDBException.of(OWNS_OVERRIDE_ANNOTATIONS_REDUNDANT, getLabel(), owns.attributeType().getLabel(), ((OwnsImpl) owns).explicitAnnotations(), owns.overridden().get().getLabel()));
        return Iterators.link(redundantRedeclarations, overridesWithRedundantAnnotations).toList();
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
            return getSubtypes(transitivity, v -> {
                switch (v.encoding()) {
                    case ENTITY_TYPE:
                        return (ThingTypeImpl) conceptMgr.convertEntityType(v);
                    case ATTRIBUTE_TYPE:
                        return (ThingTypeImpl) conceptMgr.convertAttributeType(v);
                    case RELATION_TYPE:
                        return (ThingTypeImpl) conceptMgr.convertRelationType(v);
                    case THING_TYPE:
                        if (transitivity == TRANSITIVE) {
                            assert vertex == v;
                            return this;
                        } else throw exception(TypeDBException.of(UNRECOGNISED_VALUE));
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
            if (transitivity == EXPLICIT) return emptySorted();
            else {
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
        }

        @Override
        public void setOwns(AttributeType attributeType, Set<Annotation> annotations) {
            throw exception(TypeDBException.of(ROOT_TYPE_MUTATION));
        }

        @Override
        public void setOwns(AttributeType attributeType, AttributeType overriddenType, Set<Annotation> annotations) {
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

    public static class OwnsImpl implements Owns {

        private final ThingTypeImpl owner;
        private final AttributeTypeImpl attributeType;
        private TypeEdge edge;
        private AttributeType overridden;
        private Set<Annotation> effectiveAnnotationsCache;

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
                                   @Nullable AttributeType overriddenType, Set<Annotation> annotations) {
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
                Set<Annotation> annotations
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

        public static boolean isFirstStricter(Set<TypeQLToken.Annotation> first, Set<TypeQLToken.Annotation> second) {
            if (second.contains(KEY)) {
                return false;
            } else if (second.contains(UNIQUE)) {
                return first.contains(KEY);
            } else {
                return first.contains(KEY) || first.contains(UNIQUE);
            }
        }

        public static boolean isFirstStricterOrEqual(Set<TypeQLToken.Annotation> first, Set<TypeQLToken.Annotation> second) {
            if (second.contains(KEY)) {
                return first.contains(KEY);
            } else if (second.contains(UNIQUE)) {
                return first.contains(KEY) || first.contains(UNIQUE);
            } else {
                return true;
            }
        }

        @Override
        public AttributeType attributeType() {
            return attributeType;
        }

        @Override
        public Set<Annotation> effectiveAnnotations() {
            if (owner.graphMgr().schema().isReadOnly()) {
                if (effectiveAnnotationsCache == null) effectiveAnnotationsCache = computeEffectiveAnnotations();
                return effectiveAnnotationsCache;
            } else return computeEffectiveAnnotations();
        }

        private Set<Annotation> computeEffectiveAnnotations() {
            Set<Annotation> annotations = new HashSet<>(explicitAnnotations());
            owner.getSupertype().getOwns(attributeType)
                    .or(() -> overridden().flatMap(overridden -> owner.getSupertype().getOwns(overridden)))
                    .ifPresent(owns -> annotations.addAll(owns.effectiveAnnotations()));
            // in the future, we will collapse parametrised annotations of the same type to the strictest (child) annotation
            // for example, @card(0,10) from the parent should not be kept if @card(1,5) is defined on the child
            return annotations;
        }

        public Set<Annotation> explicitAnnotations() {
            return edge.annotations();
        }

        @Override
        public Optional<AttributeType> overridden() {
            return Optional.ofNullable(overridden);
        }

        private void delete() {
            if (!edge.isDeleted()) {
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
            explicitAnnotations().stream().sorted(Comparator.comparing(Annotation::toString))
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
