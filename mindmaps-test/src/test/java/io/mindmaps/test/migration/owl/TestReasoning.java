package io.mindmaps.test.migration.owl;

import com.google.common.collect.Sets;
import io.mindmaps.concept.Concept;
import io.mindmaps.exception.MindmapsValidationException;
import io.mindmaps.graql.Graql;
import io.mindmaps.graql.MatchQuery;
import io.mindmaps.graql.QueryBuilder;
import io.mindmaps.graql.internal.reasoner.query.Query;
import io.mindmaps.graql.internal.reasoner.query.QueryAnswers;
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

import static io.mindmaps.graql.Graql.var;
import static org.junit.Assert.assertEquals;

public class TestReasoning extends TestOwlMindMapsBase {

    private IRI baseIri = IRI.create("http://www.co-ode.org/roberts/family-tree.owl");
    private OWLReasoner hermit;
    private io.mindmaps.graql.Reasoner mmReasoner;

    @Before
    public void loadOwlFiles() throws MindmapsValidationException {
        OWLOntology family = loadOntologyFromResource("owl", "family.owl");
        migrator.ontology(family).graph(graph).migrate();
        migrator.graph().commit();
        hermit = new Reasoner(new Configuration(), family);
        mmReasoner = new io.mindmaps.graql.Reasoner(migrator.graph());
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

    private QueryAnswers inferRelationMM(String relationId, String instanceId) {
        QueryBuilder qb = Graql.withGraph(migrator.graph());

        long mmStartTime = System.currentTimeMillis();
        String subjectRoleId = "owl-subject-" + relationId;
        String objectRoleId = "owl-object-" + relationId;
        MatchQuery query = qb.match(
                var("x").isa("tPerson"),
                var("y").id("e"+instanceId),
                var().isa(relationId).rel(subjectRoleId, "x").rel(objectRoleId, "y") ).select("x");
        QueryAnswers mmAnswers = mmReasoner.resolve(query);
        long mmTime = System.currentTimeMillis() - mmStartTime;
        System.out.println("MMReasoner answers: " + mmAnswers.size() + " in " + mmTime + " ms");
        return mmAnswers;
    }

    @Test
    public void testFullReasoning(){
        QueryBuilder qb = Graql.withGraph(migrator.graph());
        String richardId = "richard_henry_steward_1897";
        String hasGreatUncleId = "op-hasGreatUncle";
        String explicitQuery = "match $x isa tPerson;{$x id 'erichard_john_bright_1962';} or {$x id 'erobert_david_bright_1965';};";
        assertEquals(inferRelationMM(hasGreatUncleId, richardId), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));

        String queryString2 = "match (owl-subject-op-hasGreatUncle: $x, owl-object-op-hasGreatUncle: $y) isa op-hasGreatUncle;$x id 'eethel_archer_1912'; select $y;";
        String explicitQuery2 = "match $y isa tPerson;"+
                "{$y id 'eharry_whitfield_1854';} or" +
                "{$y id 'ejames_whitfield_1848';} or" +
                "{$y id 'ewalter_whitfield_1863';} or" +
                "{$y id 'ewilliam_whitfield_1852';} or" +
                "{$y id 'egeorge_whitfield_1865';};";
        assertEquals(mmReasoner.resolve(new Query(queryString2, graph)), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery2)));

        String queryString3 = "match (owl-subject-op-hasGreatAunt: $x, owl-object-op-hasGreatAunt: $y) isa op-hasGreatAunt;" +
                "$x id 'emary_kate_green_1865'; select $y;";
        String explicitQuery3= "match $y isa tPerson;{$y id 'etamar_green_1810';} or" +
                "{$y id 'ezilpah_green_1810';} or {$y id 'eelizabeth_pickard_1805';} or" +
                "{$y id 'esarah_ingelby_1821';} or {$y id 'eann_pickard_1809';} or" +
                "{$y id 'esusanna_pickard_1803';} or {$y id 'emary_green_1803';} or" +
                "{$y id 'erebecca_green_1800';} or {$y id 'eann_green_1806';};";
        assertEquals(mmReasoner.resolve(new Query(queryString3, graph)), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery3)));

        String eleanorId = "eleanor_pringle_1741";
        String elisabethId = "elizabeth_clamper_1760";
        String annId = "ann_lodge_1763";
        String reeceId = "reece_bright_1993";
        String megaId = "mega_clamper_1995";
        String anneId = "anne_archer_1964";

        IRI hasAncestor = baseIri.resolve("#hasAncestor");
        IRI isAncestorOf = baseIri.resolve("#isAncestorOf");
        String hasAncestorId = "op-hasAncestor";
        String isAncestorOfId = "op-isAncestorOf";

        assertEquals(inferRelationOWL(hasAncestor, eleanorId, hermit), inferRelationMM(hasAncestorId, eleanorId));
        assertEquals(inferRelationOWL(hasAncestor, elisabethId, hermit), inferRelationMM(hasAncestorId, elisabethId));
        //assertEquals(inferRelationOWL(hasAncestor, annId, hermit), inferRelationMM(hasAncestorId, annId));

        assertEquals(inferRelationOWL(isAncestorOf, anneId, hermit), inferRelationMM(isAncestorOfId, anneId));
        assertEquals(inferRelationOWL(isAncestorOf, megaId, hermit), inferRelationMM(isAncestorOfId, megaId));
        //assertEquals(inferRelationOWL(isAncestorOf, reeceId, hermit), inferRelationMM(isAncestorOfId, reeceId));
    }
}
