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

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.concept.type.AttributeType;
import com.vaticle.typedb.core.concept.type.RelationType;
import com.vaticle.typedb.core.concept.type.RoleType;
import com.vaticle.typedb.core.concept.type.ThingType;

import javax.annotation.Nullable;
import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.PLAYS_ROLE_NOT_AVAILABLE_OVERRIDDEN;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.SCHEMA_VALIDATION_LEAKED_PLAYS;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.SCHEMA_VALIDATION_LEAKED_RELATES;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.SCHEMA_VALIDATION_OVERRIDE_PLAYS_NOT_AVAILABLE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.SCHEMA_VALIDATION_RELATES_OVERRIDE_NOT_AVAILABLE;
import static com.vaticle.typedb.core.common.parameters.Concept.Transitivity.EXPLICIT;
import static com.vaticle.typedb.core.common.parameters.Concept.Transitivity.TRANSITIVE;

public class Validation {
    public static class Relates {
        public static List<TypeDBException> validateCreate(RelationType relationType, String created, @Nullable RoleType overridden) {
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
//        ThingType player;
//        RoleType played;
//        RoleType overridden;
//
//        public Plays(ThingType player, RoleType played, RoleType overridden) {
//            this.player = player;
//            this.played = played;
//            this.overridden = overridden;
//        }
//
        public static List<TypeDBException> validateCreate(ThingType thingType, RoleType added, @Nullable RoleType overridden) {
            List<TypeDBException> exceptions = new ArrayList<>();
            if (overridden != null) {
                Set<RoleType> noLongerPlays = new HashSet<>();
                noLongerPlays.add(overridden);
//                thingType.getSubtypes(EXPLICIT).forEachRemaining(subtype -> validateNoBrokenOverrides(subtype, noLongerPlays, exceptions)); // TODO: Reintroduce to operation time?
                validateNoLeakedInstances(thingType, noLongerPlays, exceptions);
            }
            return exceptions;
        }

        public static List<TypeDBException> validateRemove(ThingType thingType, RoleType deleted) {
            List<TypeDBException> exceptions = new ArrayList<>();
            Set<RoleType> noLongerPlays = new HashSet<>();
            noLongerPlays.add(deleted);
//            thingType.getSubtypes(EXPLICIT).forEachRemaining(subtype -> validateNoBrokenOverrides(subtype, noLongerPlays, exceptions)); // TODO: Reintroduce to operation time?
            validateNoLeakedInstances(thingType, noLongerPlays, exceptions);
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

            validateNoHiddenPlaysRedeclaration(thingType, hiddenPlays, exceptions);
//            validateNoBrokenOverrides(thingType, removedPlays, exceptions);
            validateNoLeakedInstances(thingType, removedPlays, exceptions); // No need for hidden plays
            return exceptions;
        }

        private static void validateNoHiddenPlaysRedeclaration(ThingType thingType, Set<RoleType> hidden, List<TypeDBException> acc) {
            if (hidden.isEmpty()) return;
            List<RoleType> overriddenHere = new ArrayList<>();
            thingType.getPlays(EXPLICIT)
                    .filter(hidden::contains)
                    .forEachRemaining(roleType -> {
                        overriddenHere.add(roleType);
                        acc.add(TypeDBException.of(PLAYS_ROLE_NOT_AVAILABLE_OVERRIDDEN, thingType.getLabel(), roleType));
                    });
            hidden.removeAll(overriddenHere);
            thingType.getSubtypes(EXPLICIT).forEachRemaining(subtype -> validateNoHiddenPlaysRedeclaration(subtype, hidden, acc));
            hidden.addAll(overriddenHere);
        }

//        // TODO: Reintroduce to operation time?
//        private static void validateNoBrokenOverrides(ThingType thingType, Set<RoleType> removed, List<TypeDBException> acc) {
//            if (removed.isEmpty()) return;
//            List<RoleType> overriddenHere = new ArrayList<>();
//            thingType.getPlays(EXPLICIT)
//                    .forEachRemaining(roleType -> {
//                        RoleType overridden = thingType.getPlaysOverridden(roleType);
//                        if (removed.contains(overridden)) {
//                            overriddenHere.add(overridden);
//                            acc.add(TypeDBException.of(SCHEMA_VALIDATION_OVERRIDE_PLAYS_NOT_AVAILABLE, thingType.getLabel(), roleType.getLabel(), overridden.getLabel()));
//                        }
//                    });
//
//            removed.removeAll(overriddenHere);
//            thingType.getSubtypes(EXPLICIT).forEachRemaining(subtype -> validateNoBrokenOverrides(subtype, removed, acc));
//            removed.addAll(overriddenHere);
//        }

        private static void validateNoLeakedInstances(ThingType thingType, Set<RoleType> removedOrHidden, List<TypeDBException> acc) {
            if (removedOrHidden.isEmpty()) return;
            List<RoleType> redeclaredHere = new ArrayList<>();
            //TODO: Remove hidden plays? It would cause an exception in validation_removedPlays_overriddenPlaysRedeclaration anyway.
            Iterators.iterate(removedOrHidden)
                    .filter(roleType -> thingType.getInstances(EXPLICIT).anyMatch(instance -> instance.getRelations(roleType).hasNext()))
                    .forEachRemaining(roleType -> {
                        redeclaredHere.add(roleType);
                        acc.add(TypeDBException.of(SCHEMA_VALIDATION_LEAKED_PLAYS, thingType.getLabel(), roleType.getLabel()));
                    });
            removedOrHidden.removeAll(redeclaredHere);
            thingType.getSubtypes(EXPLICIT).forEachRemaining(subtype -> validateNoLeakedInstances(subtype, removedOrHidden, acc));
            removedOrHidden.addAll(redeclaredHere);
        }
    }

    public static class Owns {
        ThingType owner;
        AttributeType owned;
        AttributeType overridden;
        Set<Annotation> annotations;

        public Owns(ThingType owner, AttributeType owned, AttributeType overridden, Set<Annotation> annotations) {
            this.owner = owner;
            this.owned = owned;
            this.overridden = overridden;
            this.annotations = annotations;
        }
    }
}
