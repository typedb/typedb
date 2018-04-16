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

package ai.grakn.test.property;

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

import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;

import java.util.Collection;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;
import static org.junit.Assume.assumeThat;

/**
 * @author Felix Chapman
 */
public class PropertyUtil {

    @SuppressWarnings("unchecked")
    public static <T extends SchemaConcept> Collection<T> directSubs(T schemaConcept) {
        return schemaConcept.subs().filter(subType -> schemaConcept.equals(subType.sup())).map(o -> (T) o).collect(toList());
    }

    public static Collection<Thing> directInstances(Type type) {
        Stream<? extends Thing> indirectInstances = type.instances();
        return indirectInstances.filter(instance -> type.equals(instance.type())).collect(toList());
    }

    public static <T> T choose(Stream<? extends T> stream, long seed) {
        Set<? extends T> collection = stream.collect(Collectors.toSet());
        assumeThat(collection, not(empty()));
        return chooseWithoutCheck(collection, seed);
    }

    public static <T> T choose(String message, Collection<? extends T> collection, long seed) {
        assumeThat(message, collection, not(empty()));
        return chooseWithoutCheck(collection, seed);
    }

    private static <T> T chooseWithoutCheck(Collection<? extends T> collection, long seed) {
        int index = new Random(seed).nextInt(collection.size());
        Optional<? extends T> result = collection.stream().skip(index).findFirst();
        assert result.isPresent();
        return result.get();
    }
}
