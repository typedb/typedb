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
package ai.grakn.migration.owl;

/**
 * Enumerate default/predefined elements that need to be created
 * in Grakn regardless of the ontology being migrated.
 * 
 * @author borislav 
 *
 */
public enum OwlModel {
    THING("owl:Thing"),
    OBJECT_PROPERTY("owl:topObjectProperty"),
    SUBJECT_PROPERTY("owl:topDataProperty"),
    SUBJECT("owl-subject"),
    OBJECT("owl-object"),
    IRI("owl-iri")
    ;
    
    private final String name;
    
    OwlModel(String name) {
        this.name = name;
    }
    
    public String owlname() {
        return name;
    }
}
