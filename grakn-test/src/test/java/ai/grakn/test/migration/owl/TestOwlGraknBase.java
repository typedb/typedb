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
package ai.grakn.test.migration.owl;

import ai.grakn.GraknGraph;
import ai.grakn.GraknSession;
import ai.grakn.GraknTxType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Entity;
import ai.grakn.concept.Instance;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.migration.owl.OWLMigrator;
import ai.grakn.migration.owl.OwlModel;
import ai.grakn.test.EngineContext;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Collection;
import java.util.Optional;

import static ai.grakn.test.migration.MigratorTestUtils.getFile;

/**
 * Base class for OWL migrator unit tests: create and holds OWL manager and
 * Grakn graph, statically because they don't need to be re-initialised on a per
 * test basis.
 * 
 * @author borislav
 *
 */
public class TestOwlGraknBase {
    protected OWLOntologyManager manager;
    protected OWLMigrator migrator;

    protected GraknSession factory;
    protected GraknGraph graph;

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @ClassRule
    public static final EngineContext engine = EngineContext.startInMemoryServer();

    @Before
    public void init() {
        factory = engine.factoryWithNewKeyspace();
        graph = factory.open(GraknTxType.WRITE);
        manager = OWLManager.createOWLOntologyManager();
        migrator = new OWLMigrator();
    }

    OWLOntologyManager owlManager() {
        return manager;
    }

    protected OWLOntology loadOntologyFromResource(String component, String resource) {
        try (InputStream in = new FileInputStream(getFile(component, resource))) {
            return owlManager().loadOntologyFromOntologyDocument(in);
        }
        catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    ResourceType<String> owlIriResource(){ return graph.getResourceType(OwlModel.IRI.owlname());}

    <T> Entity getEntity(T id, ResourceType<T> rtype){
        Resource<T> iri = rtype.getResource(id);
        Instance inst = iri != null? iri.ownerInstances().stream().findFirst().orElse(null) : null;
        return inst != null? inst.asEntity() : null;
    }

    Entity getEntity(String id){ return getEntity(id, owlIriResource());}

    <T extends Concept> Optional<T> findById(Collection<T> C, String id) {
        return C.stream().filter(x -> x.equals(getEntity(id))).findFirst();
    }
}