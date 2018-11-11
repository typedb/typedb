/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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
 */

package grakn.core.graql.internal.reasoner.utils.conversion;

import grakn.core.concept.Relationship;
import grakn.core.concept.Role;
import grakn.core.concept.Thing;
import grakn.core.graql.Graql;
import grakn.core.graql.Pattern;
import grakn.core.graql.Var;
import grakn.core.graql.VarPattern;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * <p>
 * Class for conversions from {@link Relationship}s.
 * </p>
 */
class RelationshipConverter implements ConceptConverter<Relationship> {

    public Pattern pattern(Relationship concept) {
        VarPattern relationPattern = Graql.var();
        Set<Pattern> idPatterns = new HashSet<>();

        for (Map.Entry<Role, Set<Thing>> entry : concept.rolePlayersMap().entrySet()) {
            for (Thing var : entry.getValue()) {
                Var rolePlayer = Graql.var();
                relationPattern = relationPattern.rel(Graql.label(entry.getKey().label()), rolePlayer);
                idPatterns.add(rolePlayer.asUserDefined().id(var.id()));
            }
        }
        relationPattern = relationPattern.isa(Graql.label(concept.type().label()));

        Pattern pattern = relationPattern;
        for (Pattern idPattern : idPatterns) {
            pattern = pattern.and(idPattern);
        }
        return pattern;
    }
}
