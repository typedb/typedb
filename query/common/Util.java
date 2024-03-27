/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.query.common;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.concept.thing.Relation;
import com.vaticle.typedb.core.concept.thing.Thing;
import com.vaticle.typedb.core.concept.type.RelationType;
import com.vaticle.typedb.core.concept.type.RoleType;
import com.vaticle.typedb.core.pattern.constraint.thing.RelationConstraint;
import com.vaticle.typedb.core.pattern.variable.TypeVariable;

import java.util.Set;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.ROLE_TYPE_AMBIGUOUS;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.ROLE_TYPE_MISSING;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeRead.TYPE_NOT_FOUND;

public class Util {
    public static RoleType tryInferRoleType(Relation relation, Thing player, RelationConstraint.RolePlayer rolePlayer) {
        RoleType roleType;
        Set<? extends RoleType> inferred;
        if (rolePlayer.roleType().isPresent()) {
            RelationType relationType = relation.getType();
            TypeVariable var = rolePlayer.roleType().get();
            if ((roleType = relationType.getRelates(var.label().get().label())) == null) {
                throw TypeDBException.of(TYPE_NOT_FOUND, Label.of(var.label().get().label(), relationType.getLabel().name()));
            }
        } else if ((inferred = player.getType().getPlays()
                .filter(rt -> rt.getRelationType().equals(relation.getType()))
                .toSet()).size() == 1) {
            roleType = inferred.iterator().next();
        } else if (inferred.size() > 1) {
            throw TypeDBException.of(ROLE_TYPE_AMBIGUOUS, rolePlayer.player().reference());
        } else {
            throw TypeDBException.of(ROLE_TYPE_MISSING, rolePlayer.player().reference());
        }
        return roleType;
    }
}
