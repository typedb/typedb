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
import com.vaticle.typedb.core.concept.type.RelationType;
import com.vaticle.typedb.core.concept.type.RoleType;
import com.vaticle.typedb.core.concept.type.impl.ThingTypeImpl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.OVERRIDDEN_PLAYED_ROLE_NOT_AVAILABLE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.OVERRIDDEN_PLAYED_ROLE_TYPE_NOT_SUPERTYPE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.PLAYS_ROLE_NOT_AVAILABLE_OVERRIDDEN;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.RELATION_RELATES_ROLE_FROM_SUPERTYPE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.RELATION_RELATES_ROLE_NOT_AVAILABLE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.ROOT_ROLE_TYPE_CANNOT_BE_PLAYED;
import static com.vaticle.typedb.core.common.parameters.Concept.Transitivity.EXPLICIT;
import static com.vaticle.typedb.core.common.parameters.Concept.Transitivity.TRANSITIVE;

public class DeclarationValidation {
    public static class Relates {
        public static List<TypeDBException> validateAdd(RelationType relationType, String toAdd) {
            List<TypeDBException> exceptions = new ArrayList<>();
            relationType.getSupertypes().filter(superType -> !superType.equals(relationType)).flatMap(superType -> superType.getRelates(EXPLICIT))
                    .filter(roleType -> roleType.getLabel().name().equals(toAdd))
                    .map(roleType -> TypeDBException.of(RELATION_RELATES_ROLE_FROM_SUPERTYPE, roleType.getLabel(), relationType.getLabel()))
                    .forEachRemaining(exceptions::add);
            return exceptions;
        }

        public static List<TypeDBException> validateOverride(RelationType relationType, String roleLabel, String overriddenLabel) {
            List<TypeDBException> exceptions = new ArrayList<>();
            if (relationType.getSupertype().getRelates(TRANSITIVE, overriddenLabel) == null) {
                exceptions.add(TypeDBException.of(RELATION_RELATES_ROLE_NOT_AVAILABLE, roleLabel, overriddenLabel));
            }
            return exceptions;
        }
    }

    public static class Plays {

        public static List<TypeDBException> validateAdd(ThingTypeImpl thingType, RoleType roleType) {
            List<TypeDBException> exceptions = new ArrayList<>();
            if (roleType.isRoot()) {
                exceptions.add(TypeDBException.of(ROOT_ROLE_TYPE_CANNOT_BE_PLAYED));
            } else if (thingType.getSupertypes().filter(t -> !t.equals(thingType)).anyMatch(supertype -> supertype.getPlays(EXPLICIT).anyMatch(rt -> roleType.equals(supertype.getPlaysOverridden(rt))))) {
                exceptions.add(TypeDBException.of(PLAYS_ROLE_NOT_AVAILABLE_OVERRIDDEN, thingType.getLabel(), roleType.getLabel()));
            }
            return exceptions;
        }

        public static List<TypeDBException> validateOverride(ThingTypeImpl thingType, RoleType roleType, RoleType overriddenType) {
            List<TypeDBException> exceptions = new ArrayList<>();
            if (roleType.getSupertypes().noneMatch(t -> t.equals(overriddenType))) {
                exceptions.add(TypeDBException.of(OVERRIDDEN_PLAYED_ROLE_TYPE_NOT_SUPERTYPE, thingType.getLabel(), roleType.getLabel(), overriddenType.getLabel()));
            }
            if (thingType.getSupertype().getPlays(TRANSITIVE).noneMatch(t -> t.equals(overriddenType))) {
                exceptions.add(TypeDBException.of(OVERRIDDEN_PLAYED_ROLE_NOT_AVAILABLE, thingType.getLabel(), roleType.getLabel(), overriddenType.getLabel()));
            }

            Set<RoleType> notOverridable = new HashSet<>();
            thingType.getPlays(EXPLICIT).toSet(notOverridable);
            thingType.getSupertypes().filter(supertype -> !supertype.equals(thingType))
                    .flatMap(supertype -> supertype.getPlays(EXPLICIT).map(superPlays -> supertype.getPlaysOverridden(superPlays)).filter(Objects::nonNull))
                    .toSet(notOverridable);
            if (notOverridable.contains(overriddenType)) {
                exceptions.add(TypeDBException.of(OVERRIDDEN_PLAYED_ROLE_NOT_AVAILABLE, thingType.getLabel(), roleType.getLabel(), overriddenType.getLabel()));
            }
            return exceptions;
        }
    }

    public static class Owns {

    }
}
