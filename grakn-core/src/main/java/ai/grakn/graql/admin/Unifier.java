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

package ai.grakn.graql.admin;

import ai.grakn.graql.VarName;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 *
 * <p>
 * Interface for resolution unifier defined as a finite set of mappings between variables xi and terms ti:
 *
 * θ = {x1/t1, x2/t2, ..., xi/ti}.
 *
 * Both variables and terms are defined in terms of graql VarNames.
 *
 * For a set of expressions Γ, the unifier θ maps elements from Γ to a single expression φ : Γθ = {φ}.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public interface Unifier{

    /**
     * @param key specific variable
     * @return corresponding term
     */
    VarName get(VarName key);

    /**
     * add a new mapping
     * @param key variable
     * @param value term
     * @return previous value associated with key, or null if there was no mapping for key
     */
    VarName addMapping(VarName key, VarName value);


    /**
     * @return true if the set of mappings is empty
     */
    boolean isEmpty();

    Map<VarName, VarName> map();

    /**
     * @return variables present in this unifier
     */
    Set<VarName> keySet();

    /**
     * @return terms present in this unifier
     */
    Collection<VarName> values();

    /**
     *
     * @return set of mappings constituting this unifier
     */
    Set<Map.Entry<VarName, VarName>> getMappings();

    /**
     * @param key variable to be inspected for presence
     * @return true if specified key is part of a mapping
     */
    boolean containsKey(VarName key);

    /**
     * @param value term to be checked for presence
     * @return true if specified value is part of a mapping
     */
    boolean containsValue(VarName value);

    /**
     * @param d unifier to be merged with this unifier
     * @return merged unifier
     */
    Unifier merge(Unifier d);

    Unifier removeTrivialMappings();

    /**
     * @return number of mappings that consittute this unifier
     */
    int size();
}
