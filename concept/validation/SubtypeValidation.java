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

package com.vaticle.typedb.core.concept.validation;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.concept.thing.Attribute;
import com.vaticle.typedb.core.concept.thing.Thing;
import com.vaticle.typedb.core.concept.type.AttributeType;
import com.vaticle.typedb.core.concept.type.RelationType;
import com.vaticle.typedb.core.concept.type.RoleType;
import com.vaticle.typedb.core.concept.type.ThingType;
import com.vaticle.typedb.core.concept.type.impl.AttributeTypeImpl;
import com.vaticle.typedb.core.concept.type.impl.ThingTypeImpl;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.common.TypeQLToken;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.*;
import static com.vaticle.typedb.core.common.iterator.Iterators.compareSize;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.common.parameters.Concept.Transitivity.EXPLICIT;
import static com.vaticle.typedb.core.common.parameters.Concept.Transitivity.TRANSITIVE;
import static com.vaticle.typeql.lang.common.TypeQLToken.Annotation.KEY;
import static com.vaticle.typeql.lang.common.TypeQLToken.Annotation.UNIQUE;
import static java.util.Collections.emptySet;

public class SubtypeValidation {

    @SafeVarargs
    public static Optional<String> collectExceptions(List<TypeDBException>... validationErrors) {
        String combinedMessage = Stream.concat(
                Stream.of(""),
                Arrays.stream(validationErrors).flatMap(Collection::stream).map(TypeDBException::getMessage)
        ).collect(Collectors.joining("\n- "));
        return combinedMessage.isBlank() ? Optional.empty() : Optional.of(combinedMessage);
    }

    public static class Relates {

        public static List<TypeDBException> validateAdd(RelationType relationType, String addedRole) {
            List<TypeDBException> exceptions = new ArrayList<>();
            Set<String> addedRoles = Collections.singleton(addedRole);
            relationType.getSubtypes(EXPLICIT).forEachRemaining(subtype -> validateRoleNameUniqueness(subtype, addedRoles, exceptions));
            return exceptions;
        }

        public static List<TypeDBException> validateOverride(RelationType relationType, @Nullable RoleType overriddenType) {
            if (overriddenType != null) {
                List<TypeDBException> exceptions = new ArrayList<>();
                Set<RoleType> noLongerRelates = new HashSet<>();
                noLongerRelates.add(overriddenType);
                relationType.getSubtypes(EXPLICIT).forEachRemaining(subtype -> validateNoBrokenOverrides(subtype, noLongerRelates, exceptions));
                validateNoLeakedInstances(relationType, noLongerRelates, exceptions);
                return exceptions;
            } else return Collections.emptyList();
        }

        public static List<TypeDBException> validateRemove(RelationType relationType, RoleType deleted) {
            List<TypeDBException> exceptions = new ArrayList<>();
            Set<RoleType> noLongerRelates = new HashSet<>();
            noLongerRelates.add(deleted);
            relationType.getSubtypes(EXPLICIT).forEachRemaining(subtype -> validateNoBrokenOverrides(subtype, noLongerRelates, exceptions));
            validateNoLeakedInstances(relationType, noLongerRelates, exceptions);
            return exceptions;
        }

        public static List<TypeDBException> validateSetSupertype(RelationType relationType, RelationType newSupertype) {
            List<TypeDBException> exceptions = new ArrayList<>();
            Set<RoleType> noLongerRelates = new HashSet<>();
            Set<RoleType> newlyAddedRelates = new HashSet<>();
            relationType.getSupertype().getRelates(TRANSITIVE).filter(roleType -> !roleType.isRoot()).forEachRemaining(noLongerRelates::add);
            newSupertype.getRelates(TRANSITIVE).filter(roleType -> !roleType.isRoot()).forEachRemaining(newlyAddedRelates::add);
            Set<RoleType> bothSupertypesRelate = com.vaticle.typedb.common.collection.Collections.intersection(newlyAddedRelates, noLongerRelates);
            newlyAddedRelates.removeAll(bothSupertypesRelate);
            noLongerRelates.removeAll(bothSupertypesRelate);

            validateRoleNameUniqueness(relationType, iterate(newlyAddedRelates).map(roleType -> roleType.getLabel().name()).toSet(), exceptions);
            validateNoBrokenOverrides(relationType, noLongerRelates, exceptions);
            validateNoLeakedInstances(relationType, noLongerRelates, exceptions);
            return exceptions;
        }

        private static void validateRoleNameUniqueness(RelationType relationType, Set<String> toRelate, List<TypeDBException> exceptions) {
            relationType.getRelates(EXPLICIT)
                    .filter(roleType -> toRelate.contains(roleType.getLabel().name()))
                    .forEachRemaining(roleType -> {
                        exceptions.add(TypeDBException.of(RELATION_RELATES_ROLE_FROM_SUPERTYPE, roleType.getLabel().name(), relationType.getLabel()));
                    });
            relationType.getSubtypes(EXPLICIT).forEachRemaining(subtype -> validateRoleNameUniqueness(subtype, toRelate, exceptions));
        }

        private static void validateNoBrokenOverrides(RelationType relationType, Set<RoleType> noLongerRelates, List<TypeDBException> exceptions) {
            if (noLongerRelates.isEmpty()) return;
            List<RoleType> overriddenHere = new ArrayList<>();
            assert relationType.getRelates(EXPLICIT).allMatch(roleType -> (
                    (relationType.getRelatesOverridden(roleType.getLabel().name()) == null && roleType.getSupertype().isRoot()) ||
                            (relationType.getRelatesOverridden(roleType.getLabel().name()).equals(roleType.getSupertype()))
            ));
            relationType.getRelates(EXPLICIT)
                    .forEachRemaining(roleType -> {
                        RoleType overriddenRoleType = relationType.getRelatesOverridden(roleType.getLabel().name());
                        if (overriddenRoleType != null && noLongerRelates.contains(overriddenRoleType)) {
                            exceptions.add(TypeDBException.of(OVERRIDDEN_RELATED_ROLE_TYPE_NOT_INHERITED,
                                    relationType.getLabel(), roleType.getLabel(), overriddenRoleType.getLabel()));
                            overriddenHere.add(overriddenRoleType);
                        }
                    });
            noLongerRelates.removeAll(overriddenHere);
            relationType.getSubtypes(EXPLICIT).forEachRemaining(subtype -> validateNoBrokenOverrides(subtype, noLongerRelates, exceptions));
            noLongerRelates.addAll(overriddenHere);
        }

        private static void validateNoLeakedInstances(RelationType relationType, Set<RoleType> noLongerRelates, List<TypeDBException> exceptions) {
            if (noLongerRelates.isEmpty()) return;
            List<? extends RoleType> overriddenHere = relationType.getRelates(EXPLICIT)
                    .map(roleType -> relationType.getRelatesOverridden(roleType.getLabel().name()))
                    .filter(noLongerRelates::contains).toList();

            noLongerRelates.removeAll(overriddenHere);
            Iterators.iterate(noLongerRelates)
                    .filter(roleType -> relationType.getInstances(EXPLICIT).anyMatch(instance -> instance.getPlayers(roleType).first().isPresent()))
                    .forEachRemaining(roleType -> {
                        exceptions.add(TypeDBException.of(INVALID_UNDEFINE_RELATES_HAS_INSTANCES, relationType.getLabel(), roleType.getLabel()));
                    });

            relationType.getSubtypes(EXPLICIT).forEachRemaining(subtype -> validateNoLeakedInstances(subtype, noLongerRelates, exceptions));
            noLongerRelates.addAll(overriddenHere);
        }

        public static String format(RelationType relationType, RoleType roleType, @Nullable RoleType overridenRoleType) {
            return format(relationType.getLabel().toString(), roleType.getLabel().name(), overridenRoleType != null ? overridenRoleType.getLabel().name() : null);
        }

        public static String format(String relationType, String roleType, @Nullable String overridenRoleType) {
            return TypeQL.type(relationType).relates(roleType, overridenRoleType).toString(false);
        }

    }

    public static class Plays {

        public static List<TypeDBException> validateOverride(ThingType thingType, RoleType overriddenType) {
            if (overriddenType != null) {
                List<TypeDBException> exceptions = new ArrayList<>();
                Set<RoleType> noLongerPlays = new HashSet<>();
                noLongerPlays.add(overriddenType);
                validateNoLeakedInstances(thingType, noLongerPlays, exceptions, true);
                validateNoHiddenPlaysRedeclaration(thingType, noLongerPlays, exceptions);
                return exceptions;
            } else return Collections.emptyList();
        }

        public static List<TypeDBException> validateRemove(ThingType thingType, RoleType deleted) {
            List<TypeDBException> exceptions = new ArrayList<>();
            Set<RoleType> noLongerPlays = new HashSet<>();
            noLongerPlays.add(deleted);
            validateNoLeakedInstances(thingType, noLongerPlays, exceptions, true);
            return exceptions;
        }

        public static List<TypeDBException> validateSetSupertype(ThingType thingType, ThingType newSupertype) {
            List<TypeDBException> exceptions = new ArrayList<>();
            Set<RoleType> removedPlays = new HashSet<>();
            Set<RoleType> hiddenPlays = new HashSet<>();
            thingType.getSupertype().getPlays(TRANSITIVE).forEachRemaining(removedPlays::add);
            newSupertype.getPlays(TRANSITIVE).forEachRemaining(removedPlays::remove);
            newSupertype.getSupertypes().forEachRemaining(t -> {
                t.getPlays(EXPLICIT).forEachRemaining(roleType -> {
                    RoleType overridden = t.getPlaysOverridden(roleType);
                    if (overridden != null) hiddenPlays.add(overridden);
                });
            });
            validateNoHiddenPlaysRedeclaration(thingType, hiddenPlays, exceptions);
            validateNoLeakedInstances(thingType, removedPlays, exceptions, false);
            return exceptions;
        }

        private static void validateNoHiddenPlaysRedeclaration(ThingType thingType, Set<RoleType> toHide, List<TypeDBException> exceptions) {
            if (toHide.isEmpty()) return;
            List<RoleType> overriddenHere = new ArrayList<>();
            thingType.getPlays(EXPLICIT)
                    .forEachRemaining(roleType -> {
                        if (toHide.contains(roleType)) {
                            exceptions.add(TypeDBException.of(PLAYS_ROLE_NOT_AVAILABLE_OVERRIDDEN, thingType.getLabel(), roleType.getLabel()));
                        }
                        if (thingType.getPlaysOverridden(roleType) != null && toHide.contains(thingType.getPlaysOverridden(roleType))) {
                            overriddenHere.add(roleType); // Since validation runs before the mutation, this (correctly) won't consider the edge being added.
                        }
                    });
            toHide.removeAll(overriddenHere);
            thingType.getSubtypes(EXPLICIT).forEachRemaining(subtype -> validateNoHiddenPlaysRedeclaration(subtype, toHide, exceptions));
            toHide.addAll(overriddenHere);
        }

        private static void validateNoLeakedInstances(ThingType thingType, Set<RoleType> noLongerPlays, List<TypeDBException> exceptions, boolean isRemovingType) {
            if (noLongerPlays.isEmpty()) return;
            List<RoleType> redeclaredOrOverriddenHere = !isRemovingType ?
                    thingType.getPlays(EXPLICIT).flatMap(roleType -> Iterators.iterate(roleType, thingType.getPlaysOverridden(roleType)))
                            .filter(noLongerPlays::contains).toList() :
                    Collections.emptyList();

            noLongerPlays.removeAll(redeclaredOrOverriddenHere);
            Iterators.iterate(noLongerPlays)
                    .filter(roleType -> thingType.getInstances(EXPLICIT).anyMatch(instance -> instance.getRelations(roleType).first().isPresent()))
                    .forEachRemaining(roleType -> {
                        exceptions.add(TypeDBException.of(INVALID_UNDEFINE_PLAYS_HAS_INSTANCES, thingType.getLabel(), roleType.getLabel()));
                    });
            thingType.getSubtypes(EXPLICIT).forEachRemaining(subtype -> validateNoLeakedInstances(subtype, noLongerPlays, exceptions, false));
            noLongerPlays.addAll(redeclaredOrOverriddenHere);
        }

        public static String format(ThingType thingType, RoleType roleType, @Nullable RoleType overridenRoleType) {
            return (overridenRoleType != null ?
                    TypeQL.type(thingType.getLabel().toString()).plays(roleType.getLabel().scope().get(), roleType.getLabel().name(), overridenRoleType.getLabel().name()) :
                    TypeQL.type(thingType.getLabel().toString()).plays(roleType.getLabel().scope().get(), roleType.getLabel().name())
            ).toString(false);
        }
    }

    public static class Owns {

        public static List<TypeDBException> validateAdd(ThingType thingType, AttributeType attributeType, Set<TypeQLToken.Annotation> explicitAnnotations) {
            List<TypeDBException> exceptions = new ArrayList<>();
            Set<TypeQLToken.Annotation> existingEffectiveAnnotations = thingType.getOwns(attributeType).map(ThingType.Owns::effectiveAnnotations).orElse(emptySet());
            // Making sure it's stricter than existingEffective is done before, since that is part of validating the declaration.
            if (ThingTypeImpl.OwnsImpl.isFirstStricter(explicitAnnotations, existingEffectiveAnnotations)) {
                Map<AttributeType, Set<TypeQLToken.Annotation>> addedOwnsAnnotations = Map.of(attributeType, explicitAnnotations);
                thingType.getSubtypes(EXPLICIT).forEachRemaining(subtype -> validateAnnotationsStricter(subtype, addedOwnsAnnotations, exceptions));
                validateDataSatisfyAnnotations(thingType, addedOwnsAnnotations, exceptions);
            }
            return exceptions;
        }

        public static List<TypeDBException> validateOverride(ThingType thingType, @Nullable AttributeType overriddenType) {
            if (overriddenType != null) {
                List<TypeDBException> exceptions = new ArrayList<>();
                Set<AttributeType> hiddenOwns = new HashSet<>();
                hiddenOwns.add(overriddenType);
                validateNoHiddenOwnsRedeclaration(thingType, hiddenOwns, exceptions);
                return exceptions;
            } else return Collections.emptyList();
        }

        public static List<TypeDBException> validateRemove(ThingType thingType, AttributeType attributeType) {
            List<TypeDBException> exceptions = new ArrayList<>();
            Set<AttributeType> removedOwns = new HashSet<>();
            removedOwns.add(attributeType);
            validateNoLeakedInstances(thingType, removedOwns, exceptions, true);
            return exceptions;
        }

        public static List<TypeDBException> validateSetSupertype(ThingType thingType, ThingType newSupertype) {
            List<TypeDBException> exceptions = new ArrayList<>();
            Set<AttributeType> removedOwns = new HashSet<>(thingType.getSupertype().getOwnedAttributes(TRANSITIVE));
            removedOwns.removeAll(newSupertype.getOwnedAttributes(TRANSITIVE));
            Set<AttributeType> hiddenOwns = new HashSet<>();
            newSupertype.getSupertypes().forEachRemaining(t -> {
                iterate(t.getOwnedAttributes(EXPLICIT)).forEachRemaining(attributeType -> {
                    AttributeType overridden = t.getOwnsOverridden(attributeType);
                    if (overridden != null) hiddenOwns.add(overridden);
                });
            });

            Map<AttributeType, Set<TypeQLToken.Annotation>> addedOwnsAnnotations = new HashMap<>();
            newSupertype.getOwns(TRANSITIVE).forEach(owns -> addedOwnsAnnotations.put(owns.attributeType(), owns.effectiveAnnotations()));

            validateNoHiddenOwnsRedeclaration(thingType, hiddenOwns, exceptions);
            validateAnnotationsStricter(thingType, addedOwnsAnnotations, exceptions);
            validateDataSatisfyAnnotations(thingType, addedOwnsAnnotations, exceptions);
            validateNoLeakedInstances(thingType, removedOwns, exceptions, false);
            return exceptions;
        }

        private static void validateNoHiddenOwnsRedeclaration(ThingType thingType, Set<AttributeType> toHide, List<TypeDBException> exceptions) {
            List<AttributeType> overriddenHere = new ArrayList<>();
            iterate(thingType.getOwnedAttributes(EXPLICIT))
                    .forEachRemaining(attributeType -> {
                        if (toHide.contains(attributeType)) {
                            exceptions.add(TypeDBException.of(OWNS_ATTRIBUTE_WAS_OVERRIDDEN, thingType.getLabel(), attributeType.getLabel()));
                        }
                        if (thingType.getOwnsOverridden(attributeType) != null && toHide.contains(thingType.getOwnsOverridden(attributeType))) {
                            overriddenHere.add(attributeType);
                        }
                    });

            toHide.removeAll(overriddenHere);
            thingType.getSubtypes(EXPLICIT).forEachRemaining(subtype -> validateNoHiddenOwnsRedeclaration(subtype, toHide, exceptions));
            toHide.addAll(overriddenHere);
        }

        private static void validateNoLeakedInstances(ThingType thingType, Set<AttributeType> noLongerOwned, List<TypeDBException> exceptions, boolean isRemovingThingType) {
            if (noLongerOwned.isEmpty()) return;
            List<AttributeType> redeclaredOrOverriddenHere = !isRemovingThingType ?
                    iterate(thingType.getOwnedAttributes(EXPLICIT)).flatMap(attributeType -> Iterators.iterate(attributeType, attributeType.getSupertype())).filter(noLongerOwned::contains).toList() :
                    Collections.emptyList();

            noLongerOwned.removeAll(redeclaredOrOverriddenHere);
            Iterators.iterate(noLongerOwned)
                    .filter(attributeType -> thingType.getInstances(EXPLICIT).anyMatch(instance -> instance.getHas(attributeType).first().isPresent()))
                    .forEachRemaining(attributeType -> {
                        exceptions.add(TypeDBException.of(INVALID_UNDEFINE_OWNS_HAS_INSTANCES, thingType.getLabel(), attributeType.getLabel()));
                    });
            thingType.getSubtypes(EXPLICIT).forEachRemaining(subtype -> validateNoLeakedInstances(subtype, noLongerOwned, exceptions, false));
            noLongerOwned.addAll(redeclaredOrOverriddenHere);
        }

        private static Set<AttributeType> overridesChain(ThingType.Owns owns) {
            Set<AttributeType> overridesChain = new HashSet<>();
            ThingType.Owns at = owns;
            while (at != null) {
                overridesChain.add(at.attributeType());
                if (at.overridden().isPresent()) {
                    at = at.owner().getSupertype().getOwns(at.overridden().get()).orElse(null);
                } else at = null;
            }
            return overridesChain;
        }

        private static void validateAnnotationsStricter(ThingType thingType, Map<AttributeType, Set<TypeQLToken.Annotation>> annotationsToAdd, List<TypeDBException> exceptions) {
            iterate(thingType.getOwns(EXPLICIT)).forEachRemaining(declaredOwns -> {
                Set<TypeQLToken.Annotation> declaredAnnotations = ((ThingTypeImpl.OwnsImpl) declaredOwns).explicitAnnotations();
                if (declaredAnnotations.isEmpty()) return;  // If no annotations are declared, they are inherited.

                List<Set<TypeQLToken.Annotation>> newInheritedAnnotationsList = Iterators.iterate(overridesChain(declaredOwns))
                        .map(attributeType -> annotationsToAdd.getOrDefault(attributeType, null)).filter(Objects::nonNull)
                        .toList();
                assert newInheritedAnnotationsList.size() <= 1;
                newInheritedAnnotationsList.forEach(newInheritedAnnotations -> {
                    if (!ThingTypeImpl.OwnsImpl.isFirstStricterOrEqual(declaredOwns.effectiveAnnotations(), newInheritedAnnotations)) {
                        exceptions.add(TypeDBException.of(OWNS_ANNOTATION_LESS_STRICT_THAN_PARENT,
                                thingType.getLabel(), declaredOwns.attributeType().getLabel(), declaredAnnotations, newInheritedAnnotations));
                    }
                });
            });
            // Can we do an 'overriddenHere' optimisation here?
            thingType.getSubtypes(EXPLICIT).forEachRemaining(subtype -> validateAnnotationsStricter(subtype, annotationsToAdd, exceptions));
        }

        private static void validateDataSatisfyAnnotations(ThingType thingType, Map<AttributeType, Set<TypeQLToken.Annotation>> annotationsToAdd, List<TypeDBException> exceptions) {
            if (annotationsToAdd.isEmpty()) return;
            annotationsToAdd.forEach((modifiedAttributeType, updatedAnnotations) -> {
                Map<AttributeType, Set<TypeQLToken.Annotation>> affectedAttributes = new HashMap<>();
                iterate(thingType.getOwns(TRANSITIVE))
                        .filter(owns -> Iterators.iterate(overridesChain(owns)).anyMatch(modifiedAttributeType::equals))
                        .forEachRemaining(owns -> affectedAttributes.put(owns.attributeType(), owns.effectiveAnnotations()));
                if (affectedAttributes.isEmpty()) {
                    affectedAttributes.put(modifiedAttributeType, Collections.emptySet());
                }
                affectedAttributes.forEach((attributeType, existingAnnotations) -> {
                    validateDataAnnotations((ThingTypeImpl) thingType, (AttributeTypeImpl) attributeType, updatedAnnotations, existingAnnotations, exceptions);
                });
            });
            // Can we do an 'overriddenHere' optimisation here?
            thingType.getSubtypes(EXPLICIT).forEachRemaining(subtype -> validateDataSatisfyAnnotations(subtype, annotationsToAdd, exceptions));
        }

        private static void validateDataAnnotations(ThingTypeImpl owner, AttributeTypeImpl attributeType,
                                                    Set<TypeQLToken.Annotation> annotations, Set<TypeQLToken.Annotation> existingAnnotations, List<TypeDBException> exceptions) {
            if (annotations.contains(KEY)) {
                if (existingAnnotations.isEmpty()) {
                    owner.getInstances(EXPLICIT).forEachRemaining(instance -> validateDataKey(owner, instance, attributeType, exceptions));
                } else if (existingAnnotations.contains(UNIQUE)) {
                    owner.getInstances(EXPLICIT).forEachRemaining(instance -> validateDataKeyCardinality(owner, instance, attributeType, exceptions));
                } else {
                    assert existingAnnotations.contains(KEY);
                }
            } else if (annotations.contains(UNIQUE)) {
                if (existingAnnotations.isEmpty()) {
                    owner.getInstances(EXPLICIT).forEachRemaining(instance -> validateDataUnique(owner, instance, attributeType, exceptions));
                } else {
                    assert existingAnnotations.contains(KEY) || existingAnnotations.contains(UNIQUE);
                }
            }
        }

        private static void validateDataKey(ThingTypeImpl ownerType, Thing owner, AttributeType attributeType, List<TypeDBException> exceptions) {
            FunctionalIterator<? extends Attribute> attrs = owner.getHas(attributeType);
            if (!attrs.hasNext()) {
                exceptions.add(TypeDBException.of(OWNS_KEY_PRECONDITION_OWNERSHIP_KEY_TOO_MANY, ownerType.getLabel(), attributeType.getLabel()));
            } else {
                Attribute key = attrs.next();
                if (attrs.hasNext()) {
                    exceptions.add(TypeDBException.of(OWNS_KEY_PRECONDITION_OWNERSHIP_KEY_MISSING, ownerType.getLabel(), attributeType.getLabel()));
                } else if (compareSize(key.getOwners(ownerType), 1) != 0) {
                    exceptions.add(TypeDBException.of(OWNS_KEY_PRECONDITION_UNIQUENESS, attributeType.getLabel(), ownerType.getLabel()));
                }
            }
        }

        private static void validateDataKeyCardinality(ThingTypeImpl ownerType, Thing owner, AttributeTypeImpl attributeType, List<TypeDBException> exceptions) {
            FunctionalIterator<? extends Attribute> attrs = owner.getHas(attributeType);
            if (!attrs.hasNext()) {
                exceptions.add(TypeDBException.of(OWNS_KEY_PRECONDITION_OWNERSHIP_KEY_TOO_MANY, ownerType.getLabel(), attributeType.getLabel()));
            } else {
                Attribute key = attrs.next();
                if (attrs.hasNext()) {
                    exceptions.add(TypeDBException.of(OWNS_KEY_PRECONDITION_OWNERSHIP_KEY_MISSING, ownerType.getLabel(), attributeType.getLabel()));
                }
            }
        }

        private static void validateDataUnique(ThingTypeImpl ownerType, Thing instance, AttributeTypeImpl attributeType, List<TypeDBException> exceptions) {
            instance.getHas(attributeType).forEachRemaining(attr -> {
                if (compareSize(attr.getOwners(ownerType), 1) != 0) {
                    exceptions.add(TypeDBException.of(OWNS_UNIQUE_PRECONDITION, attributeType.getLabel(), ownerType.getLabel()));
                }
            });
        }

        public static String format(ThingType thingType, AttributeType attributeType, @Nullable AttributeType overriddenType, Set<TypeQLToken.Annotation> annotations) {
            return (overriddenType != null ?
                    TypeQL.type(thingType.getLabel().toString()).owns(attributeType.getLabel().toString(), overriddenType.getLabel().toString(), annotations.toArray(new TypeQLToken.Annotation[0])) :
                    TypeQL.type(thingType.getLabel().toString()).owns(attributeType.getLabel().toString(), annotations.toArray(new TypeQLToken.Annotation[0]))
            ).toString(false);
        }
    }
}
