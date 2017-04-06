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

import ai.grakn.GraknGraph;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Instance;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.TypeLabel;
import ai.grakn.exception.GraknValidationException;
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

import java.util.Optional;
import java.util.function.Supplier;

/**
 * 
 * <p>
 * The OWL migrator is the main driver an OWL migration process: configure with the ontology to migrate, the
 * target Grakn graph and instance and hit go with the {@link OWLMigrator#migrate()}
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public class OWLMigrator {
    
    private Namer namer;
    private OWLOntology ontology;
    private GraknGraph graph;

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
    
    public OWLMigrator graph(GraknGraph graph) {
        this.graph = graph;
        return this;
    }
    
    public GraknGraph graph() {
        return graph;
    }
    
    public void migrate() throws GraknValidationException {
        OwlGraknGraphStoringVisitor visitor = new OwlGraknGraphStoringVisitor(this);
        visitor.prepareOWL();
        ontology.axioms().forEach(ax -> {
            ax.accept(visitor); 
        });
        graph.commit();
    }

    public ResourceType.DataType<?> owlBuiltInToGraknDatatype(OWL2Datatype propertyType) {
        if (propertyType == OWL2Datatype.XSD_BOOLEAN) {
            return ResourceType.DataType.BOOLEAN;
        } else if (propertyType == OWL2Datatype.XSD_FLOAT ||
                 propertyType == OWL2Datatype.XSD_DOUBLE ||
                 propertyType == OWL2Datatype.OWL_REAL ||
                 propertyType == OWL2Datatype.OWL_RATIONAL ||
                 propertyType == OWL2Datatype.XSD_DECIMAL) {
            return ResourceType.DataType.DOUBLE;
        } else if (propertyType.isNumeric()) {
            return ResourceType.DataType.LONG;
        } else {
            return ResourceType.DataType.STRING;
        }
    }
    
    public EntityType owlThingEntityType() {
        return graph.putEntityType(
                namer.classEntityTypeLabel(
                        ontology.getOWLOntologyManager().getOWLDataFactory().getOWLClass(
                                OwlModel.THING.owlname()).getIRI()));
    }

    public ResourceType<String> owlIriResource(){
        return graph.putResourceType(OwlModel.IRI.owlname(), ResourceType.DataType.STRING);
    }

    public <T> Entity getEntity(T id, ResourceType<T> rtype){
        Resource<T> iri = rtype.getResource(id);
        Instance inst = iri != null? iri.ownerInstances().stream().findFirst().orElse(null) : null;
        return inst != null? inst.asEntity() : null;
    }

    public Entity putEntity(String id, EntityType type) {
        Entity current = getEntity(id, owlIriResource());
        if(current != null) return current;

        TypeLabel hasIriResourceId = TypeLabel.of(OwlModel.IRI.owlname());
        ResourceType<String> iriResource = owlIriResource();
        RoleType hasIriOwner = entityRole(type, iriResource);
        RoleType hasIriValue = resourceRole(iriResource);
        RelationType hasIriRelation = graph.putRelationType(namer.resourceRelation(hasIriResourceId))
                .relates(hasIriOwner).relates(hasIriValue);

        Entity entity = type.addEntity();
        Resource resourceInstance = iriResource.putResource(id);
        hasIriRelation.addRelation()
                .addRolePlayer(hasIriOwner, entity)
                .addRolePlayer(hasIriValue, resourceInstance);
        return entity;
    }
    
    public EntityType entityType(OWLClass owlclass) {
        EntityType type = graph.putEntityType(namer.classEntityTypeLabel(owlclass.getIRI()));
        EntityType thing = owlThingEntityType();
        if (Schema.MetaSchema.isMetaLabel(type.superType().getLabel()) && !type.equals(thing)) {
            type.superType(thing);
        }
        return type;
    }

    public Entity entity(OWLNamedIndividual individual) {
        String id = namer.individualEntityName(individual.getIRI());
        Entity entity = graph.getConcept(ConceptId.of(id));
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

    public RelationType relation(OWLObjectProperty property) {
        RelationType relType = graph.putRelationType(namer.objectPropertyName(property.getIRI()));
        RoleType subjectRole = subjectRole(relType);
        RoleType objectRole = objectRole(relType);
        relType.relates(subjectRole);
        relType.relates(objectRole);
        EntityType top = this.owlThingEntityType();
        top.plays(subjectRole);
        top.plays(objectRole);
        return relType;
    }

    public RelationType relation(OWLDataProperty property) {
        RelationType relType = graph.putRelationType(namer.resourceRelation(property.getIRI()));
        ResourceType<?> resourceType = resourceType(property);
        relType.relates(entityRole(owlThingEntityType(), resourceType));
        relType.relates(resourceRole(resourceType));
        return relType;     
    }

    public RelationType relation(OWLAnnotationProperty property) {
        RelationType relType = graph.putRelationType(namer.resourceRelation(property.getIRI()));
        ResourceType<?> resourceType = graph.putResourceType(namer.fromIri(property.getIRI()), ResourceType.DataType.STRING);
        relType.relates(entityRole(owlThingEntityType(), resourceType));
        relType.relates(resourceRole(resourceType));
        return relType;
    }
    
    public RoleType subjectRole(RelationType relType) {
        return graph.putRoleType(namer.subjectRole(relType.getLabel()));
    }

    public RoleType objectRole(RelationType relType) {
        return graph.putRoleType(namer.objectRole(relType.getLabel()));
    }

    public RoleType entityRole(EntityType entityType, ResourceType<?> resourceType) {
        RoleType roleType = graph.putRoleType(namer.entityRole(resourceType.getLabel()));
        entityType.plays(roleType);
        return roleType;
    }
    
    public RoleType resourceRole(ResourceType<?> resourceType) {
        RoleType roleType = graph.putRoleType(namer.resourceRole(resourceType.getLabel()));
        resourceType.plays(roleType);
        return roleType;
    }
    
    public ResourceType<?> resourceType(OWLDataProperty property) {
        OWL2Datatype propertyType= eval(() -> {         
            Optional<OWLDataPropertyRangeAxiom> ax = ontology.dataPropertyRangeAxioms(property)
                .filter(rangeAxiom -> rangeAxiom.getRange().isOWLDatatype() &&
                                      rangeAxiom.getRange().asOWLDatatype().isBuiltIn())
                .findFirst();
            return ax.isPresent() ? ax.get().getRange().asOWLDatatype().getBuiltInDatatype() : null;
        });
        ResourceType.DataType<?> graknType = propertyType == null ? ResourceType.DataType.STRING : owlBuiltInToGraknDatatype(propertyType);
        ResourceType<?> resourceType = graph.putResourceType(namer.fromIri(property.getIRI()), graknType);
        return resourceType;        
    }

    private static class DefaultNamer implements Namer {
    }
}