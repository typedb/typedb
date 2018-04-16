/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.reasoner.utils.conversion;

/*-
 * #%L
 * grakn-graql
 * %%
 * Copyright (C) 2016 - 2018 Grakn Labs Ltd
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import ai.grakn.concept.Relationship;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * <p>
 * Class for conversions from {@link Relationship}s.
 * </p>
 * @author Kasper Piskorski
 */
class RelationshipConverter implements ConceptConverter<Relationship> {

    public Pattern pattern(Relationship concept) {
        VarPattern relationPattern = Graql.var();
        Set<Pattern> idPatterns = new HashSet<>();

        for (Map.Entry<Role, Set<Thing>> entry : concept.allRolePlayers().entrySet()) {
            for (Thing var : entry.getValue()) {
                Var rolePlayer = Graql.var();
                relationPattern = relationPattern.rel(Graql.label(entry.getKey().getLabel()), rolePlayer);
                idPatterns.add(rolePlayer.asUserDefined().id(var.getId()));
            }
        }
        relationPattern = relationPattern.isa(Graql.label(concept.type().getLabel()));

        Pattern pattern = relationPattern;
        for (Pattern idPattern : idPatterns) {
            pattern = pattern.and(idPattern);
        }
        return pattern;
    }
}
