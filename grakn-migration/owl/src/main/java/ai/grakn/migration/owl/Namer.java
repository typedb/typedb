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

import ai.grakn.concept.Label;
import ai.grakn.concept.RelationshipType;
import ai.grakn.util.Schema;
import org.semanticweb.owlapi.model.IRI;

/**
 * <p>
 * A <code>Namer</code> provides functions to generate Grakn schema names (IDs for
 * types, relations, roles etc.) from OWL ontology elements. All methods have default
 * implementations.
 * </p>
 * <p>
 * The implementation is not configurable: future work may provide for configuring prefixes, 
 * or some mapping schema from OWL to Grakn prefixes, including the standard OWL/RDF prefixes
 * etc. Such mapping schema should be based on some practical experience with the migration tool.
 * </p>
 * @author borislav
 *
 */
public interface Namer {
    /**
     * Create and return a name from a full IRI. By default this is just the fragment (the part after the '#'
     * sign in an IRI).
     */
    default String fromIri(IRI iri) {
        return iri.getShortForm();      
    }
    /**
     * @see {{@link #fromIri(IRI)}
     */
    default String fromIri(String iriAsString) {
        return fromIri(IRI.create(iriAsString));
    }
    /**
     * Generate the name of a Grakn entity from the IRI of an OWL individual
     */
    default String individualEntityName(IRI individualIri) {
        return "e" + fromIri(individualIri);
    }
    /**
     * Generate the name of a Grakn entity type from the IRI of an OWL class
     */
    default String classEntityTypeLabel(IRI classIri) {
        return "t" + fromIri(classIri);
    }
    /**
     * Generate the name of a Grakn relation type from the IRI of an OWL object property
     */
    default String objectPropertyName(IRI propIri) {
        return "op-" + fromIri(propIri);
    }
    /**
     * Make a name/id for the Grakn {@link RelationshipType} representing
     * a relation b/w an entity (OWL individual) and its resource (the OWL data property)
     */
    default String resourceRelation(IRI dataPropertyIRI) {
        return "has-" + dataPropertyIRI.getShortForm();
    }
    /**
     * Make a name for the role type corresponding to the object (i.e. range) of an OWL object
     * property.
     *  
     * @param relationLabel The label of the Grakn {@link RelationshipType}.
     */
    default Label objectRole(Label relationLabel) {
        return relationLabel.map(relation -> OwlModel.OBJECT.owlname() + "-" + relation);
    }
    /**
     * Make a name for the role type corresponding to the subject (i.e. domain) of an OWL object
     * property.
     *  
     * @param relationLabel The label of the Grakn {@link RelationshipType}.
     */
    default Label subjectRole(Label relationLabel) {
        return relationLabel.map(relation -> OwlModel.SUBJECT.owlname() + "-" + relation);
    }
    /**
     * The name of the entity role type in an entity-role relation representing an OWL data property
     */
    default Label entityRole(Label resourceLabel) {
        return Schema.ImplicitType.HAS_OWNER.getLabel(resourceLabel);
    }
    /**
     * Make a name for a resource relation type representing the value of an OWL data property.
     */
    default Label resourceRelation(Label resourceLabel) {
        return Schema.ImplicitType.HAS.getLabel(resourceLabel);
    }
    /**
     * Make a name for a resource role player representing the value of an OWL data property.
     */
    default Label resourceRole(Label resourceLabel) {
        return Schema.ImplicitType.HAS_VALUE.getLabel(resourceLabel);
    }
}