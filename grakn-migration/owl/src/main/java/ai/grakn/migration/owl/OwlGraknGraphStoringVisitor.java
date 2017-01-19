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

import ai.grakn.concept.Concept;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.TypeName;
import ai.grakn.exception.ConceptException;
import ai.grakn.graql.internal.reasoner.Utility;
import javafx.util.Pair;
import org.semanticweb.owlapi.model.AsOWLObjectProperty;
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
import org.semanticweb.owlapi.model.OWLEquivalentObjectPropertiesAxiom;
import org.semanticweb.owlapi.model.OWLInverseObjectPropertiesAxiom;
import org.semanticweb.owlapi.model.OWLLiteral;
import org.semanticweb.owlapi.model.OWLNamedIndividual;
import org.semanticweb.owlapi.model.OWLObjectProperty;
import org.semanticweb.owlapi.model.OWLObjectPropertyAssertionAxiom;
import org.semanticweb.owlapi.model.OWLObjectPropertyDomainAxiom;
import org.semanticweb.owlapi.model.OWLObjectPropertyExpression;
import org.semanticweb.owlapi.model.OWLObjectPropertyRangeAxiom;
import org.semanticweb.owlapi.model.OWLReflexiveObjectPropertyAxiom;
import org.semanticweb.owlapi.model.OWLSubClassOfAxiom;
import org.semanticweb.owlapi.model.OWLSubDataPropertyOfAxiom;
import org.semanticweb.owlapi.model.OWLSubObjectPropertyOfAxiom;
import org.semanticweb.owlapi.model.OWLSubPropertyChainOfAxiom;
import org.semanticweb.owlapi.model.OWLTransitiveObjectPropertyAxiom;
import org.semanticweb.owlapi.model.SWRLRule;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;


/**
 * <p>
 * This is the main class processing an OWL ontology file. It implements the OWLAPI visitor interfaces
 * to traverse all axioms and entities in the ontology and convert them into equivalent Grakn elements.
 * </p>
 * <p>
 * TODO - a lot of the logical more advanced axioms are skipped for now, waiting for the Grakn reasoning engine
 * to mature a bit. 
 * </p>
 * 
 * @author borislav
 *
 */
public class OwlGraknGraphStoringVisitor implements OWLAxiomVisitorEx<Concept>, OWLEntityVisitorEx<Concept> {
    private final OWLMigrator migrator;

    public OwlGraknGraphStoringVisitor(OWLMigrator migrator) {
        this.migrator = migrator;
    }
    
    public OwlGraknGraphStoringVisitor prepareOWL() {
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
        EntityType subtype;
        if (subclass.isOWLClass()) {
            subtype = migrator.entityType(subclass.asOWLClass());
        } else {
            // TODO - we need a strategy to support class expressions, e.g. as constraints 
            // on instances
            return null;
        }
        OWLClassExpression superclass = axiom.getSuperClass();
        EntityType supertype;
        if (superclass.isOWLClass()) {
            supertype = migrator.entityType(superclass.asOWLClass());
        } else {
            // TODO - we need a strategy to support class expressions, e.g. as constraints 
            // on instances
            return null;
        }
        if (!supertype.equals(subtype.superType())) {
            subtype.superType(supertype);
        }
        return null;
    }

    @Override
    public Concept visit(OWLObjectPropertyDomainAxiom axiom) {
        if (!axiom.getProperty().isOWLObjectProperty()) {
            return null;
        }
        RelationType objectRelation = migrator.relation(axiom.getProperty().asOWLObjectProperty());
        if (axiom.getDomain().isOWLClass()) {           
            EntityType entityType = migrator.entityType(axiom.getDomain().asOWLClass());
            RoleType domain = migrator.subjectRole(objectRelation);
            migrator.owlThingEntityType().deletePlaysRole(domain);
            entityType.playsRole(domain);
            objectRelation.hasRole(domain);
//          System.out.println("Replaced domain thing with " + entityType.getName());
        }
        return objectRelation;
    }

    @Override
    public Concept visit(OWLObjectPropertyRangeAxiom axiom) {
        if (!axiom.getProperty().isOWLObjectProperty()) {
            return null;
        }
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
        if (!axiom.getSubProperty().isOWLObjectProperty() || !axiom.getSuperProperty().isOWLObjectProperty()) {
            return null;
        }
        RelationType subRelation = migrator.relation(axiom.getSubProperty().asOWLObjectProperty());
        RelationType superRelation = migrator.relation(axiom.getSuperProperty().asOWLObjectProperty());

        Map<TypeName, TypeName> roleMap = new HashMap<>();
        roleMap.put(migrator.namer().subjectRole(superRelation.getName()), migrator.namer().subjectRole(subRelation.getName()));
        roleMap.put(migrator.namer().objectRole(superRelation.getName()), migrator.namer().objectRole(subRelation.getName()));
        Utility.createSubPropertyRule(superRelation, subRelation, roleMap, migrator.graph());

        migrator.subjectRole(subRelation).superType(migrator.subjectRole(superRelation));
        migrator.objectRole(subRelation).superType(migrator.objectRole(superRelation));
        subRelation.superType(superRelation);
        return null;
    }

    @Override
    public Concept visit(OWLSubDataPropertyOfAxiom axiom) {
        if (!axiom.getSubProperty().isOWLDataProperty() || !axiom.getSuperProperty().isOWLDataProperty()) {
            return null;
        }
        RelationType subRelation = migrator.relation(axiom.getSubProperty().asOWLDataProperty());
        RelationType superRelation = migrator.relation(axiom.getSuperProperty().asOWLDataProperty());
        subRelation.superType(superRelation);
        return null;
    }

    @Override
    public Concept visit(OWLEquivalentObjectPropertiesAxiom axiom) {
        Set<OWLObjectPropertyExpression> properties = axiom.getAxiomWithoutAnnotations()
                            .properties().filter(AsOWLObjectProperty::isOWLObjectProperty).collect(Collectors.toSet());
        if (properties.size() != axiom.getAxiomWithoutAnnotations().properties().count()) {
            return null;
        }

        for (OWLObjectPropertyExpression property : properties) {
            RelationType relation = migrator.relation(property.asOWLObjectProperty());
            properties.forEach(prop -> {
                RelationType eqRelation = migrator.relation(prop.asOWLObjectProperty());
                if (!relation.equals(eqRelation)) {
                    Map<TypeName, TypeName> roleMap = new HashMap<>();
                    roleMap.put(migrator.namer().subjectRole(relation.getName()),
                            migrator.namer().subjectRole(eqRelation.getName()));
                    roleMap.put(migrator.namer().objectRole(relation.getName()),
                            migrator.namer().objectRole(eqRelation.getName()));
                    Utility.createSubPropertyRule(relation, eqRelation, roleMap, migrator.graph());
                }
            });
        }
        return null;
    }

    @Override
    public Concept visit(OWLInverseObjectPropertiesAxiom axiom) {
        if (!axiom.getFirstProperty().isOWLObjectProperty() || !axiom.getSecondProperty().isOWLObjectProperty()) {
            return null;
        }
        RelationType relation = migrator.relation(axiom.getFirstProperty().asOWLObjectProperty());
        RelationType inverseRelation = migrator.relation(axiom.getSecondProperty().asOWLObjectProperty());

        Map<TypeName, TypeName> roleMapFD = new HashMap<>();
        roleMapFD.put(migrator.namer().subjectRole(relation.getName()), migrator.namer().objectRole(inverseRelation.getName()));
        roleMapFD.put(migrator.namer().objectRole(relation.getName()), migrator.namer().subjectRole(inverseRelation.getName()));
        Utility.createSubPropertyRule(relation, inverseRelation, roleMapFD, migrator.graph());

        Map<TypeName, TypeName> roleMapBD = new HashMap<>();
        roleMapBD.put(migrator.namer().subjectRole(inverseRelation.getName()), migrator.namer().objectRole(relation.getName()));
        roleMapBD.put(migrator.namer().objectRole(inverseRelation.getName()), migrator.namer().subjectRole(relation.getName()));
        Utility.createSubPropertyRule(inverseRelation, relation, roleMapBD, migrator.graph());
        return null;
    }

    @Override
    public Concept visit(OWLTransitiveObjectPropertyAxiom axiom) {
        if (!axiom.getProperty().isOWLObjectProperty()) {
            return null;
        }
        RelationType relation = migrator.relation(axiom.getProperty().asOWLObjectProperty());
        Utility.createTransitiveRule(relation, migrator.namer().subjectRole(relation.getName()),
                migrator.namer().objectRole(relation.getName()), migrator.graph());
        return null;
    }

    @Override
    public Concept visit(OWLReflexiveObjectPropertyAxiom axiom) {
        if (!axiom.getProperty().isOWLObjectProperty()) {
            return null;
        }
        RelationType relation = migrator.relation(axiom.getProperty().asOWLObjectProperty());
        Utility.createReflexiveRule(relation, migrator.graph());
        return null;
    }

    @Override
    public Concept visit(OWLSubPropertyChainOfAxiom axiom) {
        if (!axiom.getSuperProperty().isOWLObjectProperty()) {
            return null;
        }
        RelationType superRelation = migrator.relation(axiom.getSuperProperty().asOWLObjectProperty());
        LinkedHashMap<RelationType, Pair<TypeName, TypeName>> chain = new LinkedHashMap<>();

        axiom.getPropertyChain().forEach(property -> {
            RelationType relation = migrator.relation(property.asOWLObjectProperty());
            chain.put(relation,  
                    new Pair<>(migrator.namer().subjectRole(relation.getName()), migrator.namer().objectRole(relation.getName())));
        });

        Utility.createPropertyChainRule(superRelation, migrator.namer().subjectRole(superRelation.getName()),
                migrator.namer().objectRole(superRelation.getName()), chain, migrator.graph());
        return null;
    }

    @Override
    public Concept visit(OWLClassAssertionAxiom axiom) {
        if (!axiom.getIndividual().isNamed()) {
            return null;
        } else {
            return migrator.entity(axiom.getIndividual().asOWLNamedIndividual());
        }
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
        return relationType.addRelation()
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
        if (resourceType.getDataType() == ResourceType.DataType.BOOLEAN) {
            value = Boolean.parseBoolean(valueAsString);
        } else if (resourceType.getDataType() == ResourceType.DataType.LONG) {
            value = Long.parseLong(valueAsString);
        } else if (resourceType.getDataType() == ResourceType.DataType.DOUBLE) {
            value = Double.parseDouble(valueAsString);
        }
        Resource resource = resourceType.putResource(value);
        RelationType propertyRelation = migrator.relation(axiom.getProperty().asOWLDataProperty());
        RoleType entityRole = migrator.entityRole(entity.type(), resource.type());
        RoleType resourceRole = migrator.resourceRole(resource.type());
        try {       
            return propertyRelation.addRelation()
                     .putRolePlayer(entityRole, entity)
                     .putRolePlayer(resourceRole, resource);
        }
        catch (ConceptException ex) {
            if (ex.getMessage().contains("The Relation with the provided role players already exists")) {
                System.err.println("[WARN] Grakn does not support multiple values per data property/resource, ignoring axiom " + axiom);
            } else {
                ex.printStackTrace(System.err);
            }
            return null;
        }
    }

    @Override 
    public Concept visit(OWLAnnotationAssertionAxiom axiom) {
        if (! (axiom.getSubject() instanceof OWLNamedIndividual) ) {
            return null;
        }
        Optional<OWLLiteral> value = axiom.getValue().asLiteral();
        if (!value.isPresent()) {
            return null;
        }
        @SuppressWarnings("unchecked")
        ResourceType<String> resourceType = (ResourceType<String>)visit(axiom.getProperty());
        Entity entity = migrator.entity((OWLNamedIndividual)axiom.getSubject());
        Resource<String> resource = resourceType.putResource(value.get().getLiteral());
        RelationType propertyRelation = migrator.relation(axiom.getProperty());
        return propertyRelation.addRelation()
                 .putRolePlayer(migrator.entityRole(entity.type(), resource.type()), entity)
                 .putRolePlayer(migrator.resourceRole(resource.type()), resource);
    }

    @Override
    public Concept visit(SWRLRule node) {
        //TODO
        return null;
    }
    
}