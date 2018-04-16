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
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.reasoner.plan.priority;

import ai.grakn.graql.internal.reasoner.atom.Atom;

/**
 *
 * <p>
 * Class defining resolution weights for different atom configurations.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class AtomPriority {

    public static int priority(Atom atom){
        if(atom.isRelation()){
            return new RelationPriority(atom).computePriority();
        } else if (atom.isResource()){
            return new ResourcePriority(atom).computePriority();
        } else if (atom.isType()){
            return new TypePriority(atom).computePriority();
        } else {
            return new BasePriority(atom).computePriority();
        }
    }
}
