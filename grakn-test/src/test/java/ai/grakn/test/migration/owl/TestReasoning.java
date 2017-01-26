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

import com.google.common.collect.Sets;
import ai.grakn.concept.Concept;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.internal.reasoner.query.QueryAnswers;
import ai.grakn.migration.owl.OwlModel;
import java.util.stream.Stream;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.semanticweb.HermiT.Configuration;
import org.semanticweb.HermiT.Reasoner;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLClassExpression;
import org.semanticweb.owlapi.model.OWLDataFactory;
import org.semanticweb.owlapi.model.OWLNamedIndividual;
import org.semanticweb.owlapi.model.OWLObjectProperty;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.reasoner.OWLReasoner;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.graql.Graql.var;
import static org.junit.Assert.assertEquals;

public class TestReasoning extends TestOwlGraknBase {

    private IRI baseIri = IRI.create("http://www.co-ode.org/roberts/family-tree.owl");
    private OWLReasoner hermit;
    private ai.grakn.graql.Reasoner graknReasoner;

    @Before
    public void loadOwlFiles() throws GraknValidationException {
        OWLOntology family = loadOntologyFromResource("owl", "family.owl");
        migrator.ontology(family).graph(graph).migrate();
        migrator.graph().commit();
        hermit = new Reasoner(new Configuration(), family);
        graknReasoner = new ai.grakn.graql.Reasoner(migrator.graph());
    }

    //infer all subjects of relation relationIRI with object 'instanceId'
    private QueryAnswers inferRelationOWL(IRI relationIRI, String instanceId, OWLReasoner reasoner) {
        IRI instance = baseIri.resolve("#" + instanceId);

        OWLDataFactory df = manager.getOWLDataFactory();
        OWLClass person =  df.getOWLClass(baseIri.resolve("#Person"));
        OWLObjectProperty relation = df.getOWLObjectProperty(relationIRI);

        long owlStartTime = System.currentTimeMillis();
        OWLClassExpression expr = df.getOWLObjectIntersectionOf(
                person,
                df.getOWLObjectHasValue(relation, df.getOWLNamedIndividual(instance)));
        Set<OWLNamedIndividual> owlResult = reasoner.getInstances(expr).entities().collect(Collectors.toSet());
        long owlTime = System.currentTimeMillis() - owlStartTime;

        Set<Map<String, Concept>> OWLanswers = new HashSet<>();
        owlResult.forEach(result -> {
            Map<String, Concept> resultMap = new HashMap<>();
            resultMap.put("x", migrator.entity(result));
            OWLanswers.add(resultMap);
        });

        System.out.println(reasoner.toString() + " answers: " + OWLanswers.size() + " in " + owlTime + " ms");
        return new QueryAnswers(OWLanswers);
    }

    private QueryAnswers inferRelationGrakn(String relationId, String instanceId) {
        QueryBuilder qb = migrator.graph().graql();

        long gknStartTime = System.currentTimeMillis();
        String subjectRoleId = "owl-subject-" + relationId;
        String objectRoleId = "owl-object-" + relationId;

        //match $x isa tPerson; $x has name $name;
        //$y has name 'instance';(owl-subject-relationId: $x, owl-object-relationId: $y) isa relationId;
        MatchQuery query = qb.match(
                var("x").isa("tPerson"),
                var("y").has(OwlModel.IRI.owlname(), "e"+instanceId),
                var().isa(relationId).rel(subjectRoleId, "x").rel(objectRoleId, "y") ).select("x");
        QueryAnswers gknAnswers = new QueryAnswers(graknReasoner.resolve(query, false).collect(Collectors.toSet()));
        long gknTime = System.currentTimeMillis() - gknStartTime;
        System.out.println("Grakn Reasoner answers: " + gknAnswers.size() + " in " + gknTime + " ms");
        return gknAnswers;
    }

    private void assertQueriesEqual(Stream<Map<String, Concept>> s1, Stream<Map<String, Concept>> s2) {
        assertEquals(s1.collect(Collectors.toSet()), s2.collect(Collectors.toSet()));
    }
}
