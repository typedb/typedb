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
package io.mindmaps.test.migration.owl;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

import io.mindmaps.test.migration.AbstractMindmapsMigratorTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;

import io.mindmaps.concept.Concept;
import io.mindmaps.concept.Entity;
import io.mindmaps.concept.Instance;
import io.mindmaps.concept.Relation;
import io.mindmaps.concept.RelationType;
import io.mindmaps.concept.Resource;
import io.mindmaps.concept.RoleType;
import io.mindmaps.migration.owl.OWLMigrator;

/**
 * Base class for OWL migrator unit tests: create and holds OWL manager and
 * MM graph, statically because they don't need to be re-initialized on a per
 * test basis.
 * 
 * @author borislav
 *
 */
public class TestOwlMindMapsBase extends AbstractMindmapsMigratorTest {
    protected OWLOntologyManager manager;
    protected OWLMigrator migrator;

    @Before
    public void init() {
        manager = OWLManager.createOWLOntologyManager();
    }
    
    @Before
    public void initMigrator() {
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

    protected <T extends Concept> Optional<T> findById(Collection<T> C, String id) {
        return C.stream().filter(x -> x.getId().equals(id)).findFirst();
    }
}