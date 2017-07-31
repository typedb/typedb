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

import ai.grakn.concept.OntologyConcept;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.Optional;
import java.util.Random;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;
import static org.junit.Assume.assumeThat;

/**
 * @author Felix Chapman
 */
public class PropertyUtil {

    @SuppressWarnings("unchecked")
    public static <T extends OntologyConcept> Collection<T> directSubs(T ontologyElement) {
        return ontologyElement.subs().stream().filter(subType -> ontologyElement.equals(subType.sup())).map(o -> (T) o).collect(toList());
    }

    public static Collection<OntologyConcept> indirectSupers(OntologyConcept ontologyConcept) {
        Collection<OntologyConcept> supers = Lists.newArrayList();

        do {
            supers.add(ontologyConcept);
            ontologyConcept = ontologyConcept.sup();
        } while (ontologyConcept != null);

        return supers;
    }

    public static Collection<Thing> directInstances(Type type) {
        Collection<? extends Thing> indirectInstances = type.instances();
        return indirectInstances.stream().filter(instance -> type.equals(instance.type())).collect(toList());
    }

    public static <T> T choose(Collection<? extends T> collection, long seed) {
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
