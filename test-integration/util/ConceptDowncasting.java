/*
 * Copyright (C) 2020 Grakn Labs
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package grakn.core.util;

import grakn.core.concept.impl.ConceptImpl;
import grakn.core.concept.impl.RelationImpl;
import grakn.core.concept.impl.ThingImpl;
import grakn.core.concept.impl.TypeImpl;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.Relation;
import grakn.core.kb.concept.api.Thing;
import grakn.core.kb.concept.api.Type;


/**
 * Downcast concept interfaces that are restrict access to the Impl classes that allow more complex operations
 * This is exclusively used for tests
 */
public class ConceptDowncasting {

    public static ConceptImpl concept(Concept concept) {
        if (concept instanceof ConceptImpl) {
            return (ConceptImpl) concept;
        }
        throw new ClassCastException("Cannot cast " + concept.getClass() + " to " + ConceptImpl.class);
    }

    public static ThingImpl<?, ?> thing(Thing thing) {
        if (thing instanceof ThingImpl) {
            return (ThingImpl) thing;
        }
        throw new ClassCastException("Cannot cast " + thing.getClass() + " to " + ThingImpl.class);
    }

    public static RelationImpl relation(Relation relation) {
        if (relation instanceof RelationImpl) {
            return (RelationImpl) relation;
        }
        throw new ClassCastException("Cannot cast " + relation.getClass() + " to " + RelationImpl.class);
    }

    public static TypeImpl<?, ?> type(Type type){
        if (type instanceof TypeImpl) {
            return (TypeImpl) type;
        }
        throw new ClassCastException("Cannot cast " + type.getClass() + " to " + TypeImpl.class);
    }

}
