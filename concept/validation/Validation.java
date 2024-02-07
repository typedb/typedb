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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.OWNS_ATTRIBUTE_WAS_OVERRIDDEN;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.OWNS_KEY_PRECONDITION_OWNERSHIP_KEY_MISSING;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.OWNS_KEY_PRECONDITION_OWNERSHIP_KEY_TOO_MANY;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.OWNS_KEY_PRECONDITION_UNIQUENESS;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.OWNS_OVERRIDE_ANNOTATION_LESS_STRICT_THAN_PARENT;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.OWNS_REDECLARATION_ANNOTATION_LESS_STRICT;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.OWNS_UNIQUE_PRECONDITION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.PLAYS_ROLE_NOT_AVAILABLE_OVERRIDDEN;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.SCHEMA_VALIDATION_LEAKED_OWNS;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.SCHEMA_VALIDATION_LEAKED_PLAYS;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.SCHEMA_VALIDATION_LEAKED_RELATES;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.SCHEMA_VALIDATION_RELATES_OVERRIDE_NOT_AVAILABLE;
import static com.vaticle.typedb.core.common.iterator.Iterators.compareSize;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.common.parameters.Concept.Transitivity.EXPLICIT;
import static com.vaticle.typedb.core.common.parameters.Concept.Transitivity.TRANSITIVE;
import static com.vaticle.typeql.lang.common.TypeQLToken.Annotation.KEY;
import static com.vaticle.typeql.lang.common.TypeQLToken.Annotation.UNIQUE;
import static java.util.Collections.emptySet;

public class Validation {
    public static void throwIfNonEmpty(List<TypeDBException> validationErrors, Function<String, TypeDBException> exceptionFromErrorList) {
        if (!validationErrors.isEmpty()) {
            String formattedErrors = "\n- " + String.join("\n-", validationErrors.toString());
            throw exceptionFromErrorList.apply(formattedErrors);
        }
    }

    public static class Relates {

        public static String format(RelationType relationType, RoleType roleType, @Nullable RoleType overridenRoleType) {
            return format(relationType.getLabel().toString(), roleType.getLabel().name(), overridenRoleType != null ? overridenRoleType.getLabel().name() : null);
        }
        public static String format(String relationType, String roleType, @Nullable String overridenRoleType) {
            return TypeQL.type(relationType).relates(roleType, overridenRoleType).toString(false);
        }

        public static List<TypeDBException> validateAdd(RelationType relationType, String added, @Nullable RoleType overridden) {
            List<TypeDBException> exceptions = new ArrayList<>();
            if (overridden != null) {
                Set<RoleType> noLongerRelates = new HashSet<>();
                noLongerRelates.add(overridden);
                relationType.getSubtypes(EXPLICIT).forEachRemaining(subtype -> validateNoBrokenOverrides(subtype, noLongerRelates, exceptions));
                validateNoLeakedInstances(relationType, noLongerRelates, exceptions);
            }
            return exceptions;
        }

        public static List<TypeDBException> validateRemove(RelationType relationType, RoleType deleted) {
            List<TypeDBException> exceptions = new ArrayList<>();
            Set<RoleType> noLongerRelates = new HashSet<>();
            noLongerRelates.add(deleted);
            relationType.getSubtypes(EXPLICIT).forEachRemaining(subtype -> validateNoBrokenOverrides(subtype, noLongerRelates, exceptions));
            validateNoLeakedInstances(relationType, noLongerRelates, exceptions);
            return exceptions;
        }

        public static List<TypeDBException> validateRelocate(RelationType relationType, RelationType newSupertype) {
            List<TypeDBException> exceptions = new ArrayList<>();
            Set<RoleType> noLongerRelates = new HashSet<>();
            RelationType oldSuperType = relationType.getSupertype();
            oldSuperType.getRelates(TRANSITIVE).filter(roleType -> !roleType.isRoot()).forEachRemaining(noLongerRelates::add);
            newSupertype.getRelates(TRANSITIVE).forEachRemaining(noLongerRelates::remove);
            validateNoBrokenOverrides(relationType, noLongerRelates, exceptions);
            validateNoLeakedInstances(relationType, noLongerRelates, exceptions);
            return exceptions;
        }

        private static void validateNoBrokenOverrides(RelationType relationType, Set<RoleType> removed, List<TypeDBException> acc) {
            // TODO: Optimisation: any RoleType in removed which was overridden by this type need not be validated in subtypes
            relationType.getRelates(EXPLICIT)
                    .filter(roleType -> removed.contains(roleType.getSupertype()))
                    .forEachRemaining(roleType -> {
                        acc.add(TypeDBException.of(SCHEMA_VALIDATION_RELATES_OVERRIDE_NOT_AVAILABLE, roleType.getSupertype().getLabel(), roleType.getSupertype().getLabel()));
                    });
            relationType.getSubtypes(EXPLICIT).forEachRemaining(subtype -> validateNoBrokenOverrides(subtype, removed, acc));
        }

        private static void validateNoLeakedInstances(RelationType relationType, Set<RoleType> removed, List<TypeDBException> acc) {
            // TODO: Optimisation: any RoleType in removed which was overridden by this type need not be validated in subtypes
            Iterators.iterate(removed)
                    .filter(roleType -> relationType.getInstances(EXPLICIT).anyMatch(instance -> instance.getPlayers(roleType).hasNext()))
                    .forEachRemaining(roleType -> {
                        acc.add(TypeDBException.of(SCHEMA_VALIDATION_LEAKED_RELATES, relationType.getLabel(), roleType.getLabel()));
                    });
            relationType.getSubtypes(EXPLICIT).forEachRemaining(subtype -> validateNoLeakedInstances(subtype, removed, acc));
        }
    }

    public static class Plays {

        public static String format(ThingType thingType, RoleType roleType, @Nullable RoleType overridenRoleType) {
            return (overridenRoleType != null ?
                    TypeQL.type(thingType.getLabel().toString()).plays(roleType.getLabel().scope().get(), roleType.getLabel().name(), overridenRoleType.getLabel().name()) :
                    TypeQL.type(thingType.getLabel().toString()).plays(roleType.getLabel().scope().get(), roleType.getLabel().name())
                    ).toString(false);
        }

        public static List<TypeDBException> validateAdd(ThingType thingType, RoleType added, @Nullable RoleType overridden) {
            List<TypeDBException> exceptions = new ArrayList<>();
            if (overridden != null) {
                Set<RoleType> noLongerPlays = new HashSet<>();
                noLongerPlays.add(overridden);
                validateNoLeakedInstances(thingType, noLongerPlays, exceptions, true);
                validateNoHiddenPlaysRedeclaration(thingType, noLongerPlays, exceptions, true);
            }
            return exceptions;
        }

        public static List<TypeDBException> validateRemove(ThingType thingType, RoleType deleted) {
            List<TypeDBException> exceptions = new ArrayList<>();
            Set<RoleType> noLongerPlays = new HashSet<>();
            noLongerPlays.add(deleted);
            validateNoLeakedInstances(thingType, noLongerPlays, exceptions, true);
            return exceptions;
        }

        public static List<TypeDBException> validateRelocate(ThingType thingType, ThingType newSupertype) {
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

            validateNoHiddenPlaysRedeclaration(thingType, hiddenPlays, exceptions, false);
            validateNoLeakedInstances(thingType, removedPlays, exceptions, false);
            return exceptions;
        }

        private static void validateNoHiddenPlaysRedeclaration(ThingType thingType, Set<RoleType> hidden, List<TypeDBException> acc, boolean isHidingThingType) {
            if (hidden.isEmpty()) return;
            List<RoleType> overriddenHere = new ArrayList<>();
            thingType.getPlays(EXPLICIT)
                    .filter(hidden::contains)
                    .forEachRemaining(roleType -> {
                        if (!isHidingThingType) overriddenHere.add(roleType);
                        acc.add(TypeDBException.of(PLAYS_ROLE_NOT_AVAILABLE_OVERRIDDEN, thingType.getLabel(), roleType.getLabel()));
                    });
            hidden.removeAll(overriddenHere);
            thingType.getSubtypes(EXPLICIT).forEachRemaining(subtype -> validateNoHiddenPlaysRedeclaration(subtype, hidden, acc, false));
            hidden.addAll(overriddenHere);
        }

        private static void validateNoLeakedInstances(ThingType thingType, Set<RoleType> removedOrHidden, List<TypeDBException> acc, boolean isRemovingThingType) {
            List<RoleType> redeclaredHere = !isRemovingThingType ?
                    iterate(thingType.getPlays(EXPLICIT)).filter(removedOrHidden::contains).toList() :
                    Collections.emptyList();
            removedOrHidden.removeAll(redeclaredHere);
            if (!removedOrHidden.isEmpty()) {
                Iterators.iterate(removedOrHidden)
                        .filter(roleType -> thingType.getInstances(EXPLICIT).anyMatch(instance -> instance.getRelations(roleType).hasNext()))
                        .forEachRemaining(roleType -> {
                            acc.add(TypeDBException.of(SCHEMA_VALIDATION_LEAKED_PLAYS, thingType.getLabel(), roleType.getLabel()));
                        });
                thingType.getSubtypes(EXPLICIT).forEachRemaining(subtype -> validateNoLeakedInstances(subtype, removedOrHidden, acc, false));
            }
            removedOrHidden.addAll(redeclaredHere);
        }
    }

    public static class Owns {

        public static String format(ThingType thingType, AttributeType attributeType, @Nullable AttributeType overriddenType, Set<TypeQLToken.Annotation> annotations) {
            return TypeQL.type(thingType.getLabel().toString()).owns(
                    attributeType.getLabel().toString(),
                    overriddenType != null ? overriddenType.getLabel().toString() : null,
                    annotations.toArray(new TypeQLToken.Annotation[0])
            ).toString(false);
        }

        public static List<TypeDBException> validateAdd(ThingType thingType, AttributeType attributeType, @Nullable AttributeType overriddenType, Set<TypeQLToken.Annotation> explicitAnnotations) {
            List<TypeDBException> exceptions = new ArrayList<>();
            if (overriddenType != null) {
                Set<AttributeType> hiddenOwns = new HashSet<>();
                hiddenOwns.add(overriddenType);
                validateNoHiddenOwnsRedeclaration(thingType, hiddenOwns, exceptions);
            }

            Optional<ThingType.Owns> existingOrInherited = iterate(thingType.getOwns()).filter(owns -> owns.attributeType().equals(attributeType)).first();
            Optional<ThingType.Owns> existingExplicit = iterate(thingType.getOwns(EXPLICIT))
                    .filter(ownsExplicit -> ownsExplicit.attributeType().equals(attributeType)).first();
            Set<TypeQLToken.Annotation> existingEffectiveAnnotations = existingExplicit.map(ThingType.Owns::effectiveAnnotations).orElse(existingOrInherited.map(ThingType.Owns::effectiveAnnotations).orElse(emptySet()));

            // Making sure it's stricter than existingEffective is done before, since that is part of validating the declaration.
            if (ThingTypeImpl.OwnsImpl.compareAnnotationsPermissive(explicitAnnotations, existingEffectiveAnnotations) < 0) {
                Map<AttributeType, Set<TypeQLToken.Annotation>> addedOwnsAnnotations = Map.of(attributeType, explicitAnnotations);
                thingType.getSubtypes(EXPLICIT).forEachRemaining(subtype -> validateOwnsRedeclarationsAndOverridesHaveStricterAnnotations(subtype, addedOwnsAnnotations, exceptions));
                validateDataSatisfyAnnotations(thingType, addedOwnsAnnotations, exceptions);
            }
            return exceptions;
        }

        public static List<TypeDBException> validateRemove(ThingType thingType, AttributeType attributeType) {
            List<TypeDBException> exceptions = new ArrayList<>();
            Set<AttributeType> removedOwns = new HashSet<>();
            removedOwns.add(attributeType);
            validateNoLeakedInstances(thingType, removedOwns, exceptions, true);
            return exceptions;
        }

        public static List<TypeDBException> validateRelocate(ThingType thingType, ThingType newSupertype) {
            List<TypeDBException> exceptions = new ArrayList<>();
            Set<AttributeType> removedOwns = new HashSet<>(thingType.getSupertype().getOwnedAttributes(TRANSITIVE));
            removedOwns.removeAll(newSupertype.getOwnedAttributes(TRANSITIVE));
            Set<AttributeType> hiddenOwns = new HashSet<>();
            Iterators.link(Iterators.iterate(newSupertype), newSupertype.getSupertypes()).forEachRemaining(t -> {
                iterate(t.getOwnedAttributes(EXPLICIT)).forEachRemaining(attributeType -> {
                    AttributeType overridden = t.getOwnsOverridden(attributeType);
                    if (overridden != null) hiddenOwns.add(overridden);
                });
            });

            Map<AttributeType, Set<TypeQLToken.Annotation>> addedOwnsAnnotations = new HashMap<>();
            newSupertype.getOwns(TRANSITIVE).forEach(owns -> addedOwnsAnnotations.put(owns.attributeType(), owns.effectiveAnnotations()));

            validateNoHiddenOwnsRedeclaration(thingType, hiddenOwns, exceptions);
            validateOwnsRedeclarationsAndOverridesHaveStricterAnnotations(thingType, addedOwnsAnnotations, exceptions);
            validateDataSatisfyAnnotations(thingType, addedOwnsAnnotations, exceptions);
            validateNoLeakedInstances(thingType, removedOwns, exceptions, false);
            return exceptions;
        }

        private static void validateNoHiddenOwnsRedeclaration(ThingType thingType, Set<AttributeType> hiddenOwns, List<TypeDBException> acc) {
            iterate(thingType.getOwnedAttributes(EXPLICIT))
                    .filter(hiddenOwns::contains)
                    .forEachRemaining(attributeType -> {
                        acc.add(TypeDBException.of(OWNS_ATTRIBUTE_WAS_OVERRIDDEN, thingType.getLabel(), attributeType.getLabel()));
                    });
            // TODO: Optimise overridden
            thingType.getSubtypes(EXPLICIT).forEachRemaining(subtype -> validateNoHiddenOwnsRedeclaration(subtype, hiddenOwns, acc));
        }

        private static void validateOwnsRedeclarationsAndOverridesHaveStricterAnnotations(ThingType thingType, Map<AttributeType, Set<TypeQLToken.Annotation>> addedAnnotations, List<TypeDBException> acc) {
            // TODO: (LONG-TERM) We would ideally have done this at commit time, but we can't because annotations are inherited when they're not specified.
            // Concretely: when we move a type without annotations to be a descendant of one with annotations, We cannot detect the change at commit time.
            iterate(thingType.getOwns(EXPLICIT))
                    .forEachRemaining(declaredOwns -> {
                        if (addedAnnotations.containsKey(declaredOwns.attributeType())) {
                            Set<TypeQLToken.Annotation> parentAnnotations = addedAnnotations.get(declaredOwns.attributeType());
                            if (ThingTypeImpl.OwnsImpl.compareAnnotationsPermissive(declaredOwns.effectiveAnnotations(), parentAnnotations) > 0) {
                                acc.add(TypeDBException.of(OWNS_REDECLARATION_ANNOTATION_LESS_STRICT, thingType.getLabel(), declaredOwns.attributeType().getLabel(), ((ThingTypeImpl.OwnsImpl) declaredOwns).explicitAnnotations(), parentAnnotations));
                            }
                        } else if (thingType.getOwnsOverridden(declaredOwns.attributeType()) != null && addedAnnotations.containsKey(thingType.getOwnsOverridden(declaredOwns.attributeType()))) {
                            Set<TypeQLToken.Annotation> parentAnnotations = addedAnnotations.get(thingType.getOwnsOverridden(declaredOwns.attributeType()));
                            if (ThingTypeImpl.OwnsImpl.compareAnnotationsPermissive(declaredOwns.effectiveAnnotations(), parentAnnotations) > 0) {
                                acc.add(TypeDBException.of(OWNS_OVERRIDE_ANNOTATION_LESS_STRICT_THAN_PARENT, thingType.getLabel(), declaredOwns.attributeType().getLabel(), ((ThingTypeImpl.OwnsImpl) declaredOwns).explicitAnnotations(), parentAnnotations));
                            }
                        }
                    });
            // TODO: Optimise overridden
            thingType.getSubtypes(EXPLICIT).forEachRemaining(subtype -> validateOwnsRedeclarationsAndOverridesHaveStricterAnnotations(subtype, addedAnnotations, acc));
        }

        private static void validateDataSatisfyAnnotations(ThingType thingType, Map<AttributeType, Set<TypeQLToken.Annotation>> addedAnnotations, List<TypeDBException> acc) {
            // Misses validating fresh ones
            addedAnnotations.forEach((modifiedAttributeType, updatedAnnotations) -> {
                Optional<ThingType.Owns> existingOwns = iterate(thingType.getOwns(TRANSITIVE))
                        .filter(owns -> owns.attributeType().getSupertypes().anyMatch(addedAnnotations::containsKey))
                        .first();
                Set<TypeQLToken.Annotation> existingAnnotations = existingOwns.map(ThingType.Owns::effectiveAnnotations).orElse(emptySet());
                AttributeType attributeType = existingOwns.map(ThingType.Owns::attributeType).orElse(modifiedAttributeType);
                if (existingAnnotations != null && ThingTypeImpl.OwnsImpl.compareAnnotationsPermissive(updatedAnnotations, existingAnnotations) < 0) {
                    try {
                        validateData((ThingTypeImpl) thingType, (AttributeTypeImpl) attributeType, updatedAnnotations, existingAnnotations);
                    } catch (TypeDBException e) {
                        acc.add(e);
                    }
                }
            });
            // TODO: Optimise overridden
            thingType.getSubtypes(EXPLICIT).forEachRemaining(subtype -> validateDataSatisfyAnnotations(subtype, addedAnnotations, acc));
        }

        private static void validateNoLeakedInstances(ThingType thingType, Set<AttributeType> removedOwns, List<TypeDBException> acc, boolean isRemovingThingType) {
            List<AttributeType> redeclaredHere = !isRemovingThingType ?
                    iterate(thingType.getOwnedAttributes(EXPLICIT)).filter(removedOwns::contains).toList() :
                    Collections.emptyList();
            removedOwns.removeAll(redeclaredHere);
            if (!removedOwns.isEmpty()) {
                Iterators.iterate(removedOwns)
                        .filter(attributeType -> thingType.getInstances(EXPLICIT).anyMatch(instance -> instance.getHas(attributeType).hasNext()))
                        .forEachRemaining(attributeType -> {
                            acc.add(TypeDBException.of(SCHEMA_VALIDATION_LEAKED_OWNS, thingType.getLabel(), attributeType.getLabel()));
                        });
                thingType.getSubtypes(EXPLICIT).forEachRemaining(subtype -> validateNoLeakedInstances(subtype, removedOwns, acc, false));
            }
            removedOwns.addAll(redeclaredHere);
        }

        private static void validateData(ThingTypeImpl owner, AttributeTypeImpl attributeType,
                                         Set<TypeQLToken.Annotation> annotations, Set<TypeQLToken.Annotation> existingAnnotations) {
            if (annotations.contains(KEY)) {
                if (existingAnnotations.isEmpty()) {
                    owner.getInstances(EXPLICIT).forEachRemaining(instance -> validateDataKey(owner, instance, attributeType));
                } else if (existingAnnotations.contains(UNIQUE)) {
                    owner.getInstances(EXPLICIT).forEachRemaining(instance -> validateDataKeyCardinality(owner, instance, attributeType));
                } else {
                    assert existingAnnotations.contains(KEY);
                }
            } else if (annotations.contains(UNIQUE)) {
                if (existingAnnotations.isEmpty()) {
                    owner.getInstances(EXPLICIT).forEachRemaining(instance -> validateDataUnique(owner, instance, attributeType));
                } else {
                    assert existingAnnotations.contains(KEY) || existingAnnotations.contains(UNIQUE);
                }
            }
        }

        private static void validateDataKey(ThingTypeImpl ownerType, Thing owner, AttributeType attributeType) {
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
    }
}
