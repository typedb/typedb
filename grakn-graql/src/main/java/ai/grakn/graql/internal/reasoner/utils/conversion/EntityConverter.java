/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
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

import ai.grakn.concept.Entity;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Var;

/**
 * <p>
 * Class for conversions from {@link Entity}s.
 * </p>
 * @author Kasper Piskorski
 */
class EntityConverter implements ConceptConverter<Entity>{

    @Override
    public Pattern pattern(Entity concept) {
            Var entityVar = Graql.var();
            return entityVar
                    .isa(concept.type().getLabel().getValue())
                    .and(entityVar.asUserDefined().id(concept.getId()));
    }
}
