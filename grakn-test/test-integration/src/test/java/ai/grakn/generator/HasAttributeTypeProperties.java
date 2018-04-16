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

/*-
 * #%L
 * test-integration
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

import ai.grakn.concept.Label;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.pattern.property.HasAttributeTypeProperty;
import ai.grakn.graql.internal.pattern.property.LabelProperty;

/**
 * @author Felix Chapman
 */
public class HasAttributeTypeProperties extends AbstractGenerator<HasAttributeTypeProperty> {

    public HasAttributeTypeProperties() {
        super(HasAttributeTypeProperty.class);
    }

    @Override
    public HasAttributeTypeProperty generate() {
        VarPatternAdmin varPattern = gen(VarPatternAdmin.class);

        // Make sure the var has a label (or else `new HasAttributeTypeProperty(..)` will throw)
        // TODO: can we avoid this
        if (!varPattern.hasProperty(LabelProperty.class)) {
            varPattern = varPattern.label(gen(Label.class)).admin();
        }

        return HasAttributeTypeProperty.of(varPattern, random.nextBoolean());
    }
}
