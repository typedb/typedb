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

import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.Role;
import ai.grakn.exception.GraphOperationException;
import ai.grakn.graql.internal.reasoner.utils.ReasonerUtils;
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
          .relates(migrator.graph().putRole(OwlModel.OBJECT.owlname()))
          .relates(migrator.graph().putRole(OwlModel.SUBJECT.owlname()));
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
        if (!supertype.equals(subtype.sup())) {
            subtype.sup(supertype);
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
            Role domain = migrator.subjectRole(objectRelation);
            migrator.owlThingEntityType().deletePlays(domain);
            entityType.plays(domain);
            objectRelation.relates(domain);
//          System.out.println("Replaced domain thing with " + entityType.getLabel());
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
            Role range = migrator.objectRole(objectRelation);
            objectRelation.relates(range);
            migrator.owlThingEntityType().deletePlays(range);
            entityType.plays(range);
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

        Map<Label, Label> roleMap = new HashMap<>();
        roleMap.put(migrator.namer().subjectRole(superRelation.getLabel()), migrator.namer().subjectRole(subRelation.getLabel()));
        roleMap.put(migrator.namer().objectRole(superRelation.getLabel()), migrator.namer().objectRole(subRelation.getLabel()));
        ReasonerUtils.createSubPropertyRule(superRelation, subRelation, roleMap, migrator.graph());

        migrator.subjectRole(subRelation).sup(migrator.subjectRole(superRelation));
        migrator.objectRole(subRelation).sup(migrator.objectRole(superRelation));
        subRelation.sup(superRelation);
        return null;
    }

    @Override
    public Concept visit(OWLSubDataPropertyOfAxiom axiom) {
        if (!axiom.getSubProperty().isOWLDataProperty() || !axiom.getSuperProperty().isOWLDataProperty()) {
            return null;
        }
        RelationType subRelation = migrator.relation(axiom.getSubProperty().asOWLDataProperty());
        RelationType superRelation = migrator.relation(axiom.getSuperProperty().asOWLDataProperty());
        subRelation.sup(superRelation);
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
                    Map<Label, Label> roleMap = new HashMap<>();
                    roleMap.put(migrator.namer().subjectRole(relation.getLabel()),
                            migrator.namer().subjectRole(eqRelation.getLabel()));
                    roleMap.put(migrator.namer().objectRole(relation.getLabel()),
                            migrator.namer().objectRole(eqRelation.getLabel()));
                    ReasonerUtils.createSubPropertyRule(relation, eqRelation, roleMap, migrator.graph());
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

        Map<Label, Label> roleMapFD = new HashMap<>();
        roleMapFD.put(migrator.namer().subjectRole(relation.getLabel()), migrator.namer().objectRole(inverseRelation.getLabel()));
        roleMapFD.put(migrator.namer().objectRole(relation.getLabel()), migrator.namer().subjectRole(inverseRelation.getLabel()));
        ReasonerUtils.createSubPropertyRule(relation, inverseRelation, roleMapFD, migrator.graph());

        Map<Label, Label> roleMapBD = new HashMap<>();
        roleMapBD.put(migrator.namer().subjectRole(inverseRelation.getLabel()), migrator.namer().objectRole(relation.getLabel()));
        roleMapBD.put(migrator.namer().objectRole(inverseRelation.getLabel()), migrator.namer().subjectRole(relation.getLabel()));
        ReasonerUtils.createSubPropertyRule(inverseRelation, relation, roleMapBD, migrator.graph());
        return null;
    }

    @Override
    public Concept visit(OWLTransitiveObjectPropertyAxiom axiom) {
        if (!axiom.getProperty().isOWLObjectProperty()) {
            return null;
        }
        RelationType relation = migrator.relation(axiom.getProperty().asOWLObjectProperty());
        ReasonerUtils.createTransitiveRule(
                relation,
                migrator.namer().subjectRole(relation.getLabel()),
                migrator.namer().objectRole(relation.getLabel()),
                migrator.graph());
        return null;
    }

    @Override
    public Concept visit(OWLReflexiveObjectPropertyAxiom axiom) {
        if (!axiom.getProperty().isOWLObjectProperty()) {
            return null;
        }
        RelationType relation = migrator.relation(axiom.getProperty().asOWLObjectProperty());
        ReasonerUtils.createReflexiveRule(
                relation,
                migrator.namer().subjectRole(relation.getLabel()),
                migrator.namer().objectRole(relation.getLabel()),
                migrator.graph());
        return null;
    }

    @Override
    public Concept visit(OWLSubPropertyChainOfAxiom axiom) {
        if (!axiom.getSuperProperty().isOWLObjectProperty()) {
            return null;
        }
        RelationType superRelation = migrator.relation(axiom.getSuperProperty().asOWLObjectProperty());
        LinkedHashMap<RelationType, Pair<Label, Label>> chain = new LinkedHashMap<>();

        axiom.getPropertyChain().forEach(property -> {
            RelationType relation = migrator.relation(property.asOWLObjectProperty());
            chain.put(relation,  
                    new Pair<>(migrator.namer().subjectRole(relation.getLabel()), migrator.namer().objectRole(relation.getLabel())));
        });

        ReasonerUtils.createPropertyChainRule(superRelation, migrator.namer().subjectRole(superRelation.getLabel()),
                migrator.namer().objectRole(superRelation.getLabel()), chain, migrator.graph());
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
                 .addRolePlayer(migrator.subjectRole(relationType), subject)
                 .addRolePlayer(migrator.objectRole(relationType), object);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public Concept visit(OWLDataPropertyAssertionAxiom axiom) {
        if (!axiom.getProperty().isOWLDataProperty() || !axiom.getSubject().isNamed()) {
            return null;
        }
        AttributeType attributeType = migrator.resourceType(axiom.getProperty().asOWLDataProperty());
        Entity entity = migrator.entity(axiom.getSubject().asOWLNamedIndividual());
        String valueAsString =  axiom.getObject().getLiteral();
        Object value = valueAsString;
        if (attributeType.getDataType() == AttributeType.DataType.BOOLEAN) {
            value = Boolean.parseBoolean(valueAsString);
        } else if (attributeType.getDataType() == AttributeType.DataType.LONG) {
            value = Long.parseLong(valueAsString);
        } else if (attributeType.getDataType() == AttributeType.DataType.DOUBLE) {
            value = Double.parseDouble(valueAsString);
        }
        Attribute attribute = attributeType.putResource(value);
        RelationType propertyRelation = migrator.relation(axiom.getProperty().asOWLDataProperty());
        Role entityRole = migrator.entityRole(entity.type(), attribute.type());
        Role resourceRole = migrator.resourceRole(attribute.type());
        try {       
            return propertyRelation.addRelation()
                     .addRolePlayer(entityRole, entity)
                     .addRolePlayer(resourceRole, attribute);
        }
        catch (GraphOperationException ex) {
            if (ex.getMessage().contains("The Relation with the provided role players already exists")) {
                System.err.println("[WARN] Grakn does not support multiple values per data property/attribute, ignoring axiom " + axiom);
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
        AttributeType<String> attributeType = (AttributeType<String>)visit(axiom.getProperty());
        Entity entity = migrator.entity((OWLNamedIndividual)axiom.getSubject());
        Attribute<String> attribute = attributeType.putResource(value.get().getLiteral());
        RelationType propertyRelation = migrator.relation(axiom.getProperty());
        return propertyRelation.addRelation()
                 .addRolePlayer(migrator.entityRole(entity.type(), attribute.type()), entity)
                 .addRolePlayer(migrator.resourceRole(attribute.type()), attribute);
    }

    @Override
    public Concept visit(SWRLRule node) {
        //TODO
        return null;
    }
    
}