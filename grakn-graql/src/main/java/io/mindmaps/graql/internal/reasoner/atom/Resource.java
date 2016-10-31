/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */
package io.mindmaps.graql.internal.reasoner.atom;

import io.mindmaps.graql.admin.VarAdmin;
import io.mindmaps.graql.internal.pattern.property.HasResourceProperty;
import io.mindmaps.graql.internal.reasoner.query.Query;
import java.util.Set;
import java.util.stream.Collectors;

public class Resource extends Binary{

    public Resource(VarAdmin pattern) { super(pattern);}
    public Resource(VarAdmin pattern, Query par) { super(pattern, par);}
    public Resource(Resource a) { super(a);}

    @Override
    protected String extractName(VarAdmin var){
        String name = "";
        Set<HasResourceProperty> props = var.getProperties(HasResourceProperty.class).collect(Collectors.toSet());
        if(props.size() == 1){
            VarAdmin resVar = props.iterator().next().getResource();
            if (resVar.getValuePredicates().isEmpty() && resVar.isUserDefinedName())
                name = resVar.getName();
        }
        return name;
    }

    @Override
    public Atomic clone(){
        return new Resource(this);
    }

    @Override
    public boolean isResource(){ return true;}
}
