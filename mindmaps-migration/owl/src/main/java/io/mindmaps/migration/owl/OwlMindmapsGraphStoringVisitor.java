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
package io.mindmaps.migration.owl;

import io.mindmaps.concept.Concept;
import io.mindmaps.concept.Entity;
import io.mindmaps.concept.EntityType;
import io.mindmaps.concept.RelationType;
import io.mindmaps.concept.Resource;
import io.mindmaps.concept.ResourceType;
import io.mindmaps.concept.RoleType;
import io.mindmaps.exception.ConceptException;
import org.semanticweb.owlapi.model.OWLAnnotationAssertionAxiom;
import org.semanticweb.owlapi.model.OWLAnnotationProperty;
import org.semanticweb.owlapi.model.OWLAxiomVisitorEx;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLClassAssertionAxiom;
import org.semanticweb.owlapi.model.OWLClassExpression;
import org.semanticweb.owlapi.model.OWLDataProperty;
import org.semanticweb.owlapi.model.OWLDataPropertyAssertionAxiom;
import org.semanticweb.owlapi.model.OWLDeclarationAxiom;
import org.semanticweb.owlapi.model.OWLEntityVisitorEx;
import org.semanticweb.owlapi.model.OWLLiteral;
import org.semanticweb.owlapi.model.OWLNamedIndividual;
import org.semanticweb.owlapi.model.OWLObjectProperty;
import org.semanticweb.owlapi.model.OWLObjectPropertyAssertionAxiom;
import org.semanticweb.owlapi.model.OWLObjectPropertyDomainAxiom;
import org.semanticweb.owlapi.model.OWLObjectPropertyRangeAxiom;
import org.semanticweb.owlapi.model.OWLSubClassOfAxiom;
import org.semanticweb.owlapi.model.OWLSubDataPropertyOfAxiom;
import org.semanticweb.owlapi.model.OWLSubObjectPropertyOfAxiom;

import java.util.Optional;

/**
 * <p>
 * This is the main class processing an OWL ontology file. It implements the OWLAPI visitor interfaces
 * to traverse all axioms and entities in the ontology and convert them into equivalent Mindmaps elements.
 * </p>
 * <p>
 * TODO - a lot of the logical more advanced axioms are skipped for now, waiting for the Mindmaps reasoning engine
 * to mature a bit. 
 * </p>
 * 
 * @author borislav
 *
 */
public class OwlMindmapsGraphStoringVisitor implements OWLAxiomVisitorEx<Concept>, OWLEntityVisitorEx<Concept> {
    private OWLMigrator migrator;   
            
    public OwlMindmapsGraphStoringVisitor(OWLMigrator migrator) {
        this.migrator = migrator;
    }
    
    public OwlMindmapsGraphStoringVisitor prepareOWL() {
        migrator.entityType(migrator.ontology().getOWLOntologyManager().getOWLDataFactory().getOWLClass(OwlModel.THING.owlname()));
        migrator.relation(migrator.ontology().getOWLOntologyManager().getOWLDataFactory().getOWLObjectProperty(OwlModel.OBJECT_PROPERTY.owlname()))
          .hasRole(migrator.graph().putRoleType(OwlModel.OBJECT.owlname()))
          .hasRole(migrator.graph().putRoleType(OwlModel.SUBJECT.owlname()));
        return this;
    }
    
    @Override
    public Concept visit(OWLClass ce) {
        return migrator.entityType(ce);
    }

    @Override
    public Concept visit(OWLObjectProperty property) {
        return migrator.relation(property);
    }


    @Override
    public Concept visit(OWLDataProperty property) {    
        return migrator.resourceType(property);
    }


    @Override
    public Concept visit(OWLAnnotationProperty property) {
        return migrator.relation(property);
    }


    @Override
    public Concept visit(OWLNamedIndividual individual) {
        return migrator.entity(individual);
    }

    @Override
    public Concept visit(OWLDeclarationAxiom axiom) {
        return axiom.getEntity().accept(this);
    }

    @Override
    public Concept visit(OWLSubClassOfAxiom axiom) {
        OWLClassExpression subclass = axiom.getSubClass();
        EntityType subtype = null;
        if (subclass.isOWLClass())
            subtype = migrator.entityType(subclass.asOWLClass());
        else {
            // TODO - we need a strategy to support class expressions, e.g. as constraints 
            // on instances
            return null;
        }
        OWLClassExpression superclass = axiom.getSuperClass();
        EntityType supertype = null;
        if (superclass.isOWLClass())
            supertype = migrator.entityType(superclass.asOWLClass());
        else {
            // TODO - we need a strategy to support class expressions, e.g. as constraints 
            // on instances
            return null;
        }
        if (!supertype.equals(subtype.superType()))
            subtype.superType(supertype);
        return null;
    }

    @Override
    public Concept visit(OWLObjectPropertyDomainAxiom axiom) {
        if (!axiom.getProperty().isOWLObjectProperty())
            return null;
        RelationType objectRelation = migrator.relation(axiom.getProperty().asOWLObjectProperty());
        if (axiom.getDomain().isOWLClass()) {           
            EntityType entityType = migrator.entityType(axiom.getDomain().asOWLClass());
            RoleType domain = migrator.subjectRole(objectRelation);
            migrator.owlThingEntityType().deletePlaysRole(domain);
            entityType.playsRole(domain);
            objectRelation.hasRole(domain);
//          System.out.println("Replaced domain thing with " + entityType.getId());
        }
        return objectRelation;
    }

    @Override
    public Concept visit(OWLObjectPropertyRangeAxiom axiom) {
        if (!axiom.getProperty().isOWLObjectProperty())
            return null;
        RelationType objectRelation = migrator.relation(axiom.getProperty().asOWLObjectProperty());
        if (axiom.getRange().isOWLClass()) {
            EntityType entityType = migrator.entityType(axiom.getRange().asOWLClass());
            RoleType range = migrator.objectRole(objectRelation);
            objectRelation.hasRole(range);          
            migrator.owlThingEntityType().deletePlaysRole(range);
            entityType.playsRole(range);
        }       
        return objectRelation;
    }

    @Override
    public Concept visit(OWLSubObjectPropertyOfAxiom axiom) {
        if (!axiom.getSubProperty().isOWLObjectProperty() || !axiom.getSuperProperty().isOWLObjectProperty())
            return null;
        RelationType subRelation = migrator.relation(axiom.getSubProperty().asOWLObjectProperty());
        RelationType superRelation = migrator.relation(axiom.getSuperProperty().asOWLObjectProperty());
        subRelation.superType(superRelation);
        return null;
    }

    
    @Override
    public Concept visit(OWLSubDataPropertyOfAxiom axiom) {
        if (!axiom.getSubProperty().isOWLDataProperty() || !axiom.getSuperProperty().isOWLDataProperty())
            return null;
        RelationType subRelation = migrator.relation(axiom.getSubProperty().asOWLDataProperty());
        RelationType superRelation = migrator.relation(axiom.getSuperProperty().asOWLDataProperty());
        subRelation.superType(superRelation);
        return null;
    }

    @Override
    public Concept visit(OWLClassAssertionAxiom axiom) {
        if (!axiom.getIndividual().isNamed())
            return null;
        else
            return migrator.entity(axiom.getIndividual().asOWLNamedIndividual());
    }

    @Override
    public Concept visit(OWLObjectPropertyAssertionAxiom axiom) {
        if (!axiom.getSubject().isNamed() || 
            !axiom.getObject().isNamed() || 
            !axiom.getProperty().isOWLObjectProperty()) {
            return null;
        }
        Entity subject = migrator.entity(axiom.getSubject().asOWLNamedIndividual());
        Entity object = migrator.entity(axiom.getObject().asOWLNamedIndividual());
        RelationType relationType = migrator.relation(axiom.getProperty().asOWLObjectProperty());       
        return migrator.graph().addRelation(relationType)
                 .putRolePlayer(migrator.subjectRole(relationType), subject)
                 .putRolePlayer(migrator.objectRole(relationType), object);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public Concept visit(OWLDataPropertyAssertionAxiom axiom) {
        if (!axiom.getProperty().isOWLDataProperty() || !axiom.getSubject().isNamed()) {
            return null;
        }
        ResourceType resourceType = migrator.resourceType(axiom.getProperty().asOWLDataProperty());
        Entity entity = migrator.entity(axiom.getSubject().asOWLNamedIndividual());
        String valueAsString =  axiom.getObject().getLiteral();
        Object value = valueAsString;
        if (resourceType.getDataType() == ResourceType.DataType.BOOLEAN)
            value = Boolean.parseBoolean(valueAsString);
        else if (resourceType.getDataType() == ResourceType.DataType.LONG)
            value = Long.parseLong(valueAsString);
        else if (resourceType.getDataType() == ResourceType.DataType.DOUBLE)
            value = Double.parseDouble(valueAsString);
        Resource resource = migrator.graph().putResource(value, resourceType);
        RelationType propertyRelation = migrator.relation(axiom.getProperty().asOWLDataProperty());
        RoleType entityRole = migrator.entityRole(entity.type(), resource.type());
        RoleType resourceRole = migrator.resourceRole(resource.type());
        try {       
            return migrator.graph().addRelation(propertyRelation)
                     .putRolePlayer(entityRole, entity)
                     .putRolePlayer(resourceRole, resource);
        }
        catch (ConceptException ex) {
            if (ex.getMessage().contains("The Relation with the provided role players already exists"))
                System.err.println("[WARN] Mindmaps does not support multiple values per data property/resource, ignoring axiom " + axiom);
            else
                ex.printStackTrace(System.err);
            return null;
        }
    }

    @Override 
    public Concept visit(OWLAnnotationAssertionAxiom axiom) {
        if (! (axiom.getSubject() instanceof OWLNamedIndividual) )
            return null;
        Optional<OWLLiteral> value = axiom.getValue().asLiteral();
        if (!value.isPresent())
            return null;
        @SuppressWarnings("unchecked")
        ResourceType<String> resourceType = (ResourceType<String>)visit(axiom.getProperty());
        Entity entity = migrator.entity((OWLNamedIndividual)axiom.getSubject());
        Resource<String> resource = migrator.graph().putResource(value.get().getLiteral(), resourceType);
        RelationType propertyRelation = migrator.relation(axiom.getProperty());
        return migrator.graph().addRelation(propertyRelation)
                 .putRolePlayer(migrator.entityRole(entity.type(), resource.type()), entity)
                 .putRolePlayer(migrator.resourceRole(resource.type()), resource);
    }   
    
    
}