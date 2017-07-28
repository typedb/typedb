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

package ai.grakn.test.property;

import ai.grakn.concept.Resource;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.Ignore;
import org.junit.runner.RunWith;

import static ai.grakn.test.property.PropertyUtil.choose;
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

    @Ignore
    @Property
    public void whenGettingTheResourceOfAThing_TheResourcesOwnerIsTheThing(Thing thing, long seed) {
        Resource<?> resource = PropertyUtil.choose(thing.resources(), seed);
        assertTrue("[" + thing + "] is connected to resource [" + resource + "] but is not in it's owner set", resource.ownerInstances().contains(thing));
    }
}
