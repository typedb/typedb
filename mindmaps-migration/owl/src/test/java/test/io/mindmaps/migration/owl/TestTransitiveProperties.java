package test.io.mindmaps.migration.owl;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import org.junit.Assert;
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

public class TestTransitiveProperties extends TestOwlMindMapsBase {
	private IRI baseIri = IRI.create("http://www.co-ode.org/roberts/family-tree.owl");
	private OWLOntology family = null;
	
	@Before
	public void loadShakespeare() {
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
    public void testSubpropertyInference() {
    	Reasoner reasoner = new Reasoner(new Configuration(), family);
    	IRI person = baseIri.resolve("#Person");
    	IRI hasAncestor = baseIri.resolve("#hasAncestor");
    	OWLDataFactory df = manager.getOWLDataFactory();    	
    	OWLClassExpression expr = df.getOWLObjectIntersectionOf(
    				    df.getOWLClass(person), 
    					df.getOWLObjectHasValue(df.getOWLObjectProperty(hasAncestor), 
    											df.getOWLNamedIndividual(baseIri.resolve("#John"))));
    	Set<OWLNamedIndividual> owlResult = reasoner.getInstances(expr).entities().collect(Collectors.toSet());
    	System.out.println(owlResult);
    }

}
