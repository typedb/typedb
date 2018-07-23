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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package strategy;

import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Role;
import ai.grakn.concept.Type;
import pdf.PDF;
import pick.StreamProviderInterface;

/**
 *
 */
public class RolePlayerTypeStrategy extends TypeStrategy implements HasPicker {

    private final Role role;
    private final String roleLabel;
    private StreamProviderInterface<ConceptId> conceptPicker;

    public RolePlayerTypeStrategy(Role role, Type type, PDF numInstancesPDF, StreamProviderInterface<ConceptId> conceptPicker) {
        super(type, numInstancesPDF);
        this.role = role;
        this.roleLabel = role.label().getValue();
        this.conceptPicker = conceptPicker;
    }

    public StreamProviderInterface<ConceptId> getPicker() {
         return conceptPicker;
    }

    public String getRoleLabel() {
        return this.roleLabel;
    }

    public Role getRole() {
        return role;
    }
}

