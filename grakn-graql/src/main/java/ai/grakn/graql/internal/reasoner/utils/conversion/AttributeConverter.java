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

import ai.grakn.concept.Attribute;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;

/**
 * <p>
 * Class for conversions from {@link Attribute}s.
 * </p>
 * @author Kasper Piskorski
 */
class AttributeConverter implements ConceptConverter<Attribute> {

    public Pattern pattern(Attribute concept) {
        Var owner = Graql.var().asUserDefined();
        VarPattern resourceVar = Graql.var().asUserDefined().val(concept.getValue());
        return owner
                .has(concept.type().getLabel(),resourceVar)
                .id(concept.owner().getId());
    }
}
