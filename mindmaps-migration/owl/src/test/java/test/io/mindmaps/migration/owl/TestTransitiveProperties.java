package test.io.mindmaps.migration.owl;

import io.mindmaps.concept.Concept;
import io.mindmaps.graql.MatchQuery;
import io.mindmaps.graql.QueryBuilder;
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


public class TestTransitiveProperties extends TestOwlMindMapsBase {
	private IRI baseIri = IRI.create("http://www.co-ode.org/roberts/family-tree.owl");
	private OWLOntology family = null;
	
	@Before
	public void loadFamily() {
        try {
            family = loadOntologyFromResource("/io/mindmaps/migration/owl/samples/family.owl");               
            migrator.ontology(family).graph(graph).migrate();
            migrator.graph().commit();
        }
        catch (Throwable t) {
            t.printStackTrace(System.err);
            System.exit(-1);
        }		
	}
	
    @Test
    public void testTransitiveInference() {
    	Reasoner reasoner = new Reasoner(new Configuration(), family);
    	IRI person = baseIri.resolve("#Person");
    	IRI hasAncestor = baseIri.resolve("#hasAncestor");
    	OWLDataFactory df = manager.getOWLDataFactory();    	
    	OWLClassExpression expr = df.getOWLObjectIntersectionOf(
    				    df.getOWLClass(person), 
    					df.getOWLObjectHasValue(df.getOWLObjectProperty(hasAncestor), 
    											df.getOWLNamedIndividual(baseIri.resolve("#John"))));
    	Set<OWLNamedIndividual> owlResult = reasoner.getInstances(expr).entities().collect(Collectors.toSet());
		Set<Map<String, Concept>> OWLanswers = new HashSet<>();
		owlResult.forEach(result -> {
			Map<String, Concept> resultMap = new HashMap<>();
			resultMap.put("x", migrator.entity(result));
			OWLanswers.add(resultMap);
		});

		QueryBuilder qb = Graql.withGraph(migrator.graph());
		io.mindmaps.graql.Reasoner mmReasoner = new io.mindmaps.graql.Reasoner(migrator.graph());

		String queryString = "match $x isa tPerson;$y id 'eJohn';" +
							 "(owl-subject-op-hasAncestor: $x, owl-object-op-hasAncestor: $y) isa op-hasAncestor; select $x;";
		MatchQuery query = qb.parseMatch(queryString);
		assertEquals(mmReasoner.resolve(query), OWLanswers);
    }

}
