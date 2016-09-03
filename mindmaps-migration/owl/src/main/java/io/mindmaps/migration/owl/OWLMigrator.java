/*
 *  MindmapsDB - A Distributed Semantic Database
 *  Copyright (C) 2016  Mindmaps Research Ltd
 *
 *  MindmapsDB is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  MindmapsDB is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */
package io.mindmaps.migration.owl;

import java.util.Optional;
import java.util.function.Supplier;

import org.semanticweb.owlapi.model.OWLAnnotationProperty;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLClassAssertionAxiom;
import org.semanticweb.owlapi.model.OWLDataProperty;
import org.semanticweb.owlapi.model.OWLDataPropertyRangeAxiom;
import org.semanticweb.owlapi.model.OWLNamedIndividual;
import org.semanticweb.owlapi.model.OWLObjectProperty;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.vocab.OWL2Datatype;

import io.mindmaps.MindmapsTransaction;
import io.mindmaps.core.Data;
import io.mindmaps.core.MindmapsGraph;
import io.mindmaps.core.implementation.exception.MindmapsValidationException;
import io.mindmaps.core.model.Entity;
import io.mindmaps.core.model.EntityType;
import io.mindmaps.core.model.RelationType;
import io.mindmaps.core.model.ResourceType;
import io.mindmaps.core.model.RoleType;

/**
 * 
 * <p>
 * The OWL migrator is the main driver an OWL migration process: configure with the ontology to migrate, the
 * target Mindmaps graph and instance and hit go with the {@link OWLMigrator#migrate()}
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public class OWLMigrator {
	
	private Namer namer;
	private OWLOntology ontology;
	private MindmapsGraph graph;
	private MindmapsTransaction tx;
	
	private <T> T eval(Supplier<T> f) {
		return f.get();
	}
	
	public OWLMigrator() {
		this.namer = new Namer(){};
	}

	public MindmapsTransaction tx() {
		return tx;
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
	
	public OWLMigrator graph(MindmapsGraph graph) {
		this.graph = graph;
		return this;
	}
	
	public MindmapsGraph graph() {
		return graph;
	}
	
	public void migrate() throws MindmapsValidationException { 
		tx = graph.getTransaction();
		OwlMindmapsGraphStoringVisitor visitor = new OwlMindmapsGraphStoringVisitor(this); 
		visitor.prepareOWL();
		ontology.axioms().forEach(ax -> {
			ax.accept(visitor);	
		});
		tx.commit();
	}

	public Data<?> owlBuiltInToMindmapsDatatype(OWL2Datatype propertyType) {
		if (propertyType == OWL2Datatype.XSD_BOOLEAN)
			return Data.BOOLEAN;
		else if (propertyType == OWL2Datatype.XSD_FLOAT || 
				 propertyType == OWL2Datatype.XSD_DOUBLE ||
				 propertyType == OWL2Datatype.OWL_REAL ||
				 propertyType == OWL2Datatype.OWL_RATIONAL ||
				 propertyType == OWL2Datatype.XSD_DECIMAL)
			return Data.DOUBLE;
		else if (propertyType.isNumeric())
			return Data.LONG;
		else
			return Data.STRING;
	}
	
	public EntityType owlThingEntityType() {
		return tx.putEntityType(
				namer.classEntityTypeName(
						ontology.getOWLOntologyManager().getOWLDataFactory().getOWLClass(
								OwlModel.THING.owlname()).getIRI()));
	}
	
	public EntityType entityType(OWLClass owlclass) {
		EntityType type = tx.putEntityType(namer.classEntityTypeName(owlclass.getIRI()));
		EntityType thing = owlThingEntityType();
		if (type.superType() == null && !type.equals(thing))
			type.superType(thing);
		return type;
	}

	public Entity entity(OWLNamedIndividual individual) {
		String id = namer.individualEntityName(individual.getIRI());
		Entity entity = tx.getEntity(id);
		if (entity != null)
			return entity;
		OWLClass owlclass = eval(() -> {
			Optional<OWLClassAssertionAxiom> expr = ontology
					.classAssertionAxioms(individual)
					.filter(ax -> ax.getClassExpression().isOWLClass())
					.findFirst();
			return expr.isPresent() ? expr.get().getClassExpression().asOWLClass() : null;
		});
		return tx.putEntity(id, owlclass == null ? owlThingEntityType() : entityType(owlclass));		
	}	

	public RelationType relation(OWLObjectProperty property) {
		RelationType relType = tx.putRelationType(namer.objectPropertyName(property.getIRI()));
		RoleType subjectRole = subjectRole(relType);
		RoleType objectRole = objectRole(relType);
		relType.hasRole(subjectRole);
		relType.hasRole(objectRole);
		EntityType top = this.owlThingEntityType();
		top.playsRole(subjectRole);
		top.playsRole(objectRole);
		return relType;
	}

	public RelationType relation(OWLDataProperty property) {
		RelationType relType = tx.putRelationType(namer.resourceRelation(property.getIRI()));
		ResourceType<?> resourceType = resourceType(property);
		relType.hasRole(entityRole(owlThingEntityType(), resourceType));
		relType.hasRole(resourceRole(resourceType));
		return relType;		
	}

	public RelationType relation(OWLAnnotationProperty property) {
		RelationType relType = tx.putRelationType(namer.resourceRelation(property.getIRI()));
		ResourceType<?> resourceType = tx.putResourceType(namer.fromIri(property.getIRI()), Data.STRING);
		relType.hasRole(entityRole(owlThingEntityType(), resourceType));
		relType.hasRole(resourceRole(resourceType));
		return relType;
	}
	
	public RoleType subjectRole(RelationType relType) {
		return tx.putRoleType(namer.subjectRole(relType.getId()));
	}

	public RoleType objectRole(RelationType relType) {
		return tx.putRoleType(namer.objectRole(relType.getId()));
	}

	public RoleType entityRole(EntityType entityType, ResourceType<?> resourceType) {
		RoleType roleType = tx.putRoleType(namer.entityRole(resourceType.getId()));
		entityType.playsRole(roleType);
		return roleType;
	}
	
	public RoleType resourceRole(ResourceType<?> resourceType) {
		RoleType roleType = tx.putRoleType(namer.resourceRole(resourceType.getId()));
		resourceType.playsRole(roleType);
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
		Data<?> mindmapsType = propertyType == null ? Data.STRING : owlBuiltInToMindmapsDatatype(propertyType);
		ResourceType<?> resourceType = tx.putResourceType(namer.fromIri(property.getIRI()), mindmapsType);
		return resourceType;		
	}	
}