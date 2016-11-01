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

package ai.grakn.graql.internal.reasoner.atom;

import ai.grakn.concept.Concept;
import ai.grakn.graql.internal.reasoner.query.Query;
import ai.grakn.concept.Concept;
import ai.grakn.graql.Graql;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.reasoner.query.Query;

public class IdPredicate extends Predicate<String>{

    public IdPredicate(VarAdmin pattern) {
        super(pattern);
    }

    public IdPredicate(VarAdmin pattern, Query par) {
        super(pattern, par);
    }

    public IdPredicate(IdPredicate a) {
        super(a);
    }

    public IdPredicate(String varName, Concept con) {
        super(createIdPredicate(varName, con));
        this.predicate = con.getId();
    }

    public IdPredicate(String varName, Concept con, Query parent) {
        this(varName, con);
        setParentQuery(parent);
    }

    public static VarAdmin createIdPredicate(String varName, Concept con){
        return Graql.var(varName).id(con.getId()).admin();
    }

    @Override
    public Atomic clone(){
        return new IdPredicate(this);
    }

    @Override
    public boolean isIdPredicate(){ return true;}

    @Override
    public String getPredicateValue() { return predicate;}

    @Override
    protected String extractPredicate(VarAdmin var){ return var.admin().getId().orElse("");}
}
