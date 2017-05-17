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

package ai.grakn.concept;

import java.io.Serializable;

/**
 * <p>
 *     A Type Id
 * </p>
 *
 * <p>
 *     A class which represents an id of any {@link Type} in the {@link ai.grakn.GraknGraph}.
 *     Also contains a static method for producing Type IDs from Integers.
 * </p>
 *
 * @author fppt
 */
public class TypeId implements Comparable<TypeId>, Serializable {
    private static final long serialVersionUID = 3181633335040468179L;

    private Integer typeId;

    private TypeId(Integer typeId){
        this.typeId = typeId;
    }

    /**
     *
     * @return Used for indexing purposes and for graql traversals
     */
    public Integer getValue(){
        return typeId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TypeId cast = (TypeId) o;
        return typeId.equals(cast.getValue());
    }

    @Override
    public int hashCode() {
       return typeId;
    }

    @Override
    public int compareTo(TypeId o) {
        return getValue().compareTo(o.getValue());
    }

    @Override
    public String toString(){
        return getValue().toString();
    }

    public boolean isValid(){
        return typeId != -1;
    }

    /**
     *
     * @param value The integer which potentially represents a Type
     * @return The matching type ID
     */
    public static TypeId of(Integer value){
        return new TypeId(value);
    }

    /**
     * @return a type id which does not match any type
     */
    public static TypeId invalid(){
        return new TypeId(-1);
    }
}
