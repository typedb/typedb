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

package ai.grakn.generator;

import ai.grakn.concept.Label;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.pattern.property.DirectIsaProperty;
import ai.grakn.graql.internal.pattern.property.HasAttributeProperty;
import ai.grakn.graql.internal.pattern.property.IsaProperty;

/**
 * @author Felix Chapman
 */
public class HasAttributeProperties extends AbstractGenerator<HasAttributeProperty> {

    public HasAttributeProperties() {
        super(HasAttributeProperty.class);
    }

    @Override
    public HasAttributeProperty generate() {
        VarPatternAdmin varPatternAttribute;
        VarPatternAdmin varPatternRelationship;

        // `HasAttributeProperty` will implicitly attach an `IsaProperty`, so must not clash
        do {
            varPatternAttribute = gen(VarPatternAdmin.class);
        } while (varPatternAttribute.hasProperty(IsaProperty.class) ||
                varPatternAttribute.hasProperty(DirectIsaProperty.class));

        do {
            varPatternRelationship = gen(VarPatternAdmin.class);
        } while (varPatternRelationship.hasProperty(IsaProperty.class) ||
                varPatternRelationship.hasProperty(DirectIsaProperty.class));


        return HasAttributeProperty.of(gen(Label.class), varPatternAttribute, varPatternRelationship);
    }
}