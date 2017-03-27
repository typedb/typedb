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

package ai.grakn.graql.internal.reasoner.query;

import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.Unifier;
import com.google.common.collect.Maps;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 * <p>
 * Implementation of Unifier interface.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class UnifierImpl implements Unifier {

    //TODO turn it to multimap to accommodate all cases
    private Map<VarName, VarName> unifier = new HashMap<>();

    public UnifierImpl(){};
    public UnifierImpl(Map<VarName, VarName> map){
        unifier.putAll(map);
    }
    public UnifierImpl(Unifier u){
        unifier.putAll(u.map());
    }

    @Override
    public boolean equals(Object obj){
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        UnifierImpl u2 = (UnifierImpl) obj;
        return unifier.equals(u2.map());
    }

    @Override
    public int hashCode(){
        return unifier.hashCode();
    }

    @Override
    public boolean isEmpty() {
        return unifier.isEmpty();
    }

    @Override
    public Map<VarName, VarName> map() {
        return Maps.newHashMap(unifier);
    }

    @Override
    public Set<VarName> keySet() {
        return unifier.keySet();
    }

    @Override
    public Collection<VarName> values() {
        return unifier.values();
    }

    @Override
    public Set<Map.Entry<VarName, VarName>> getMappings(){ return unifier.entrySet();}

    public VarName addMapping(VarName key, VarName value){
        return unifier.put(key, value);
    }

    @Override
    public VarName get(VarName key) {
        return unifier.get(key);
    }

    @Override
    public boolean containsKey(VarName key) {
        return unifier.containsKey(key);
    }

    @Override
    public boolean containsValue(VarName value) {
        return unifier.containsValue(value);
    }

    @Override
    public Unifier merge(Unifier d) {
        unifier.putAll(d.map());
        return this;
    }

    @Override
    public Unifier removeTrivialMappings() {
        return new UnifierImpl(
                unifier.entrySet().stream()
                .filter(e -> e.getKey() != e.getValue())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))
                ;
    }

    @Override
    public int size(){ return unifier.size();}
}
