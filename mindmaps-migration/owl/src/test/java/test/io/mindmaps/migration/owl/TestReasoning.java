package test.io.mindmaps.migration.owl;

import io.mindmaps.concept.Concept;
import io.mindmaps.graql.MatchQuery;
import io.mindmaps.graql.QueryBuilder;
import io.mindmaps.graql.internal.reasoner.query.QueryAnswers;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;
import org.semanticweb.HermiT.Configuration;
import org.semanticweb.HermiT.Reasoner;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLClassExpression;
import org.semanticweb.owlapi.model.OWLDataFactory;
import org.semanticweb.owlapi.model.OWLNamedIndividual;
import org.semanticweb.owlapi.model.OWLOntology;


import io.mindmaps.graql.Graql;

import static org.junit.Assert.assertEquals;


public class TestReasoning extends TestOwlMindMapsBase {
	private IRI baseIri = IRI.create("http://www.co-ode.org/roberts/family-tree.owl");
	private OWLOntology family = null;
	private String dataPath = "/io/mindmaps/migration/owl/samples/";

    @Before
    public void loadOwlFiles() {
        try {
            family = loadOntologyFromResource(dataPath + "family.owl");
            migrator.ontology(family).graph(graph).migrate();
            migrator.graph().commit();
        }
        catch (Throwable t) {
            t.printStackTrace(System.err);
            System.exit(-1);
        }
    }

    private QueryAnswers inferHasAncestorHermit(String instanceId) {
        Reasoner reasoner = new Reasoner(new Configuration(), family);
        IRI person = baseIri.resolve("#Person");
        IRI instance = baseIri.resolve("#" + instanceId);
        IRI hasAncestor = baseIri.resolve("#hasAncestor");

        long owlStartTime = System.currentTimeMillis();
        OWLDataFactory df = manager.getOWLDataFactory();
        OWLClassExpression expr = df.getOWLObjectIntersectionOf(
                df.getOWLClass(person),
                df.getOWLObjectHasValue(df.getOWLObjectProperty(hasAncestor), df.getOWLNamedIndividual(instance)));
        Set<OWLNamedIndividual> owlResult = reasoner.getInstances(expr).entities().collect(Collectors.toSet());
        long owlTime = System.currentTimeMillis() - owlStartTime;

        Set<Map<String, Concept>> OWLanswers = new HashSet<>();
        owlResult.forEach(result -> {
            Map<String, Concept> resultMap = new HashMap<>();
            resultMap.put("x", migrator.entity(result));
            OWLanswers.add(resultMap);
        });

        System.out.println("Hermit answers: " + OWLanswers.size() + " in " + owlTime + " ms");
        return new QueryAnswers(OWLanswers);
    }

    private QueryAnswers inferHasAncestorMM(String instanceId) {
        QueryBuilder qb = Graql.withGraph(migrator.graph());
        io.mindmaps.graql.Reasoner mmReasoner = new io.mindmaps.graql.Reasoner(migrator.graph());

        long mmStartTime = System.currentTimeMillis();
        String queryString = "match $x isa tPerson;$y id 'e" + instanceId + "';" +
                "(owl-subject-op-hasAncestor: $x, owl-object-op-hasAncestor: $y) isa op-hasAncestor; select $x;";
        MatchQuery query = qb.parseMatch(queryString);
        QueryAnswers mmAnswers = mmReasoner.resolve(query);
        long mmTime = System.currentTimeMillis() - mmStartTime;
        System.out.println("MMReasoner answers: " + mmAnswers.size() + " in " + mmTime + " ms");
        return mmAnswers;
    }

	@Test
	public void testFullInference() {
        String eleanorId = "eleanor_pringle_1741";
        String elisabethId = "elizabeth_clamper_1760";
        String annId = "ann_lodge_1763";
        assertEquals(inferHasAncestorHermit(eleanorId), inferHasAncestorMM(eleanorId));
        assertEquals(inferHasAncestorHermit(elisabethId), inferHasAncestorMM(elisabethId));
        assertEquals(inferHasAncestorHermit(annId), inferHasAncestorMM(annId));
    }
}
