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

package ai.grakn.test.property.kb;

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

import ai.grakn.concept.Attribute;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.generator.AbstractThingGenerator.WithResource;
import ai.grakn.test.property.PropertyUtil;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.runner.RunWith;

import static java.util.stream.Collectors.toSet;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertThat;

/**
 * @author Felix Chapman
 */
@RunWith(JUnitQuickcheck.class)
public class ThingPropertyTest {

    @Property
    public void whenGettingTheDirectTypeOfAThing_TheThingIsADirectInstanceOfThatType(Thing thing) {
        Type type = thing.type();
        assertThat(PropertyUtil.directInstances(type), hasItem(thing));
    }

    @Property
    public void whenGettingTheResourceOfAThing_TheResourcesOwnerIsTheThing(@WithResource Thing thing, long seed) {
        Attribute<?> attribute = PropertyUtil.choose(thing.attributes(), seed);
        assertTrue("[" + thing + "] is connected to attribute [" + attribute + "] but is not in it's owner set", attribute.ownerInstances().collect(toSet()).contains(thing));
    }
}
