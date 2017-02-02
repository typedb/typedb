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
 *
 */

package ai.grakn.generator;

import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.internal.pattern.Patterns;
import com.google.common.collect.Sets;

import java.util.Set;

public class Conjunctions extends AbstractGenerator<Conjunction> {

    public Conjunctions() {
        super(Conjunction.class);
    }

    @Override
    public Conjunction<PatternAdmin> generate() {
        Set<PatternAdmin> patterns = Sets.newHashSet();

        // TODO: actuall fill conjunctions
//        int size = random.nextInt(status.size());
//        for (int i = 0; i < size; i ++) {
//            patterns.add(gen(PatternAdmin.class));
//        }

        return Patterns.conjunction(patterns);
    }
}
