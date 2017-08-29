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

import ai.grakn.GraknTx;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Thing;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.Role;
import ai.grakn.concept.Label;
import ai.grakn.exception.InvalidKBException;
import ai.grakn.util.Schema;
import org.semanticweb.owlapi.model.OWLAnnotationProperty;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLClassAssertionAxiom;
import org.semanticweb.owlapi.model.OWLDataProperty;
import org.semanticweb.owlapi.model.OWLDataPropertyRangeAxiom;
import org.semanticweb.owlapi.model.OWLNamedIndividual;
import org.semanticweb.owlapi.model.OWLObjectProperty;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.vocab.OWL2Datatype;

import javax.annotation.Nullable;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * 
 * <p>
 * The OWL migrator is the main driver an OWL migration process: configure with the ontology to migrate, the
 * target {@link GraknTx} and instance and hit go with the {@link OWLMigrator#migrate()}
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public class OWLMigrator {
    
    private Namer namer;
    private OWLOntology ontology;
    private GraknTx tx;

    private <T> T eval(Supplier<T> f) {
        return f.get();
    }
    
    public OWLMigrator() {
        this.namer = new DefaultNamer();
    }

    public OWLMigrator namer(Namer namer) {
        this.namer = namer;
        return this;
    }
    
    public Namer namer() {
        return namer;
    }   
    
    public OWLMigrator ontology(OWLOntology ontology) {
        this.ontology = ontology;
        return this;
    }
    
    public OWLOntology ontology() {
        return this.ontology;
    }
    
    public OWLMigrator tx(GraknTx graph) {
        this.tx = graph;
        return this;
    }
    
    public GraknTx tx() {
        return tx;
    }
    
    public void migrate() throws InvalidKBException {
        OwlGraknTxStoringVisitor visitor = new OwlGraknTxStoringVisitor(this);
        visitor.prepareOWL();
        ontology.axioms().forEach(ax -> ax.accept(visitor));
        tx.commit();
    }

    public AttributeType.DataType<?> owlBuiltInToGraknDatatype(OWL2Datatype propertyType) {
        if (propertyType == OWL2Datatype.XSD_BOOLEAN) {
            return AttributeType.DataType.BOOLEAN;
        } else if (propertyType == OWL2Datatype.XSD_FLOAT ||
                 propertyType == OWL2Datatype.XSD_DOUBLE ||
                 propertyType == OWL2Datatype.OWL_REAL ||
                 propertyType == OWL2Datatype.OWL_RATIONAL ||
                 propertyType == OWL2Datatype.XSD_DECIMAL) {
            return AttributeType.DataType.DOUBLE;
        } else if (propertyType.isNumeric()) {
            return AttributeType.DataType.LONG;
        } else {
            return AttributeType.DataType.STRING;
        }
    }
    
    public EntityType owlThingEntityType() {
        return tx.putEntityType(
                namer.classEntityTypeLabel(
                        ontology.getOWLOntologyManager().getOWLDataFactory().getOWLClass(
                                OwlModel.THING.owlname()).getIRI()));
    }

    public AttributeType<String> owlIriResource(){
        return tx.putAttributeType(OwlModel.IRI.owlname(), AttributeType.DataType.STRING);
    }

    @Nullable
    public <T> Entity getEntity(T id, AttributeType<T> rtype){
        Attribute<T> iri = rtype.getAttribute(id);
        Thing inst = iri != null? iri.ownerInstances().findFirst().orElse(null) : null;
        return inst != null? inst.asEntity() : null;
    }

    public Entity putEntity(String id, EntityType type) {
        Entity current = getEntity(id, owlIriResource());
        if(current != null) return current;

        Label hasIriResourceId = Label.of(OwlModel.IRI.owlname());
        AttributeType<String> iriResource = owlIriResource();
        Role hasIriOwner = entityRole(type, iriResource);
        Role hasIriValue = resourceRole(iriResource);
        RelationshipType hasIriRelation = tx.putRelationshipType(namer.resourceRelation(hasIriResourceId))
                .relates(hasIriOwner).relates(hasIriValue);

        Entity entity = type.addEntity();
        Attribute attributeInstance = iriResource.putAttribute(id);
        hasIriRelation.addRelationship()
                .addRolePlayer(hasIriOwner, entity)
                .addRolePlayer(hasIriValue, attributeInstance);
        return entity;
    }
    
    public EntityType entityType(OWLClass owlclass) {
        EntityType type = tx.putEntityType(namer.classEntityTypeLabel(owlclass.getIRI()));
        EntityType thing = owlThingEntityType();
        if (Schema.MetaSchema.isMetaLabel(type.sup().getLabel()) && !type.equals(thing)) {
            type.sup(thing);
        }
        return type;
    }

    public Entity entity(OWLNamedIndividual individual) {
        String id = namer.individualEntityName(individual.getIRI());
        Entity entity = tx.getConcept(ConceptId.of(id));
        if (entity != null) {
            return entity;
        }
        OWLClass owlclass = eval(() -> {
            Optional<OWLClassAssertionAxiom> expr = ontology
                    .classAssertionAxioms(individual)
                    .filter(ax -> ax.getClassExpression().isOWLClass())
                    .findFirst();
            return expr.isPresent() ? expr.get().getClassExpression().asOWLClass() : null;
        });
        return putEntity(id, owlclass == null ? owlThingEntityType() : entityType(owlclass));
    }   

    public RelationshipType relation(OWLObjectProperty property) {
        RelationshipType relType = tx.putRelationshipType(namer.objectPropertyName(property.getIRI()));
        Role subjectRole = subjectRole(relType);
        Role objectRole = objectRole(relType);
        relType.relates(subjectRole);
        relType.relates(objectRole);
        EntityType top = this.owlThingEntityType();
        top.plays(subjectRole);
        top.plays(objectRole);
        return relType;
    }

    public RelationshipType relation(OWLDataProperty property) {
        RelationshipType relType = tx.putRelationshipType(namer.resourceRelation(property.getIRI()));
        AttributeType<?> attributeType = resourceType(property);
        relType.relates(entityRole(owlThingEntityType(), attributeType));
        relType.relates(resourceRole(attributeType));
        return relType;     
    }

    public RelationshipType relation(OWLAnnotationProperty property) {
        RelationshipType relType = tx.putRelationshipType(namer.resourceRelation(property.getIRI()));
        AttributeType<?> attributeType = tx.putAttributeType(namer.fromIri(property.getIRI()), AttributeType.DataType.STRING);
        relType.relates(entityRole(owlThingEntityType(), attributeType));
        relType.relates(resourceRole(attributeType));
        return relType;
    }
    
    public Role subjectRole(RelationshipType relType) {
        return tx.putRole(namer.subjectRole(relType.getLabel()));
    }

    public Role objectRole(RelationshipType relType) {
        return tx.putRole(namer.objectRole(relType.getLabel()));
    }

    public Role entityRole(EntityType entityType, AttributeType<?> attributeType) {
        Role role = tx.putRole(namer.entityRole(attributeType.getLabel()));
        entityType.plays(role);
        return role;
    }
    
    public Role resourceRole(AttributeType<?> attributeType) {
        Role role = tx.putRole(namer.resourceRole(attributeType.getLabel()));
        attributeType.plays(role);
        return role;
    }
    
    public AttributeType<?> resourceType(OWLDataProperty property) {
        OWL2Datatype propertyType= eval(() -> {         
            Optional<OWLDataPropertyRangeAxiom> ax = ontology.dataPropertyRangeAxioms(property)
                .filter(rangeAxiom -> rangeAxiom.getRange().isOWLDatatype() &&
                                      rangeAxiom.getRange().asOWLDatatype().isBuiltIn())
                .findFirst();
            return ax.isPresent() ? ax.get().getRange().asOWLDatatype().getBuiltInDatatype() : null;
        });
        AttributeType.DataType<?> graknType = propertyType == null ? AttributeType.DataType.STRING : owlBuiltInToGraknDatatype(propertyType);
        AttributeType<?> attributeType = tx.putAttributeType(namer.fromIri(property.getIRI()), graknType);
        return attributeType;
    }

    private static class DefaultNamer implements Namer {
    }
}