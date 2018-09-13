/*
 *  GRAKN.AI - THE KNOWLEDGE GRAPH
 *  Copyright (C) 2018 Grakn Labs Ltd
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package strategy;

import ai.grakn.concept.RelationshipType;
import pdf.PDF;

import java.util.Set;

/**
 *
 */
public class RelationshipStrategy extends TypeStrategy<RelationshipType> {

    private Set<RolePlayerTypeStrategy> rolePlayerTypeStrategies;

    public <P extends PDF> RelationshipStrategy(RelationshipType type, P numInstancesPDF, Set<RolePlayerTypeStrategy> rolePlayerTypeStrategies) {
        super(type, numInstancesPDF);
        this.rolePlayerTypeStrategies = rolePlayerTypeStrategies;
    }

    public Set<RolePlayerTypeStrategy> getRolePlayerTypeStrategies() {
        return rolePlayerTypeStrategies;
    }
}
