package test.io.mindmaps.migration.owl;

import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;
import org.semanticweb.HermiT.Reasoner;
import org.semanticweb.HermiT.Configuration;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLNamedIndividual;
import org.semanticweb.owlapi.model.OWLOntology;

import io.mindmaps.graql.Graql;

public class TestReasoning extends TestOwlMindMapsBase {
	private IRI baseIri = IRI.create("http://www.workingontologist.org/Examples/Chapter3/shakespeare.owl");
	private OWLOntology ontology = null;
	private Reasoner reasoner = null;
	
	@Before
	public void loadShakespeare() {
        try {
            ontology = loadOntologyFromResource("/io/mindmaps/migration/owl/samples/shakespeare.owl");   
            reasoner = new Reasoner(new Configuration(), ontology);
            migrator.ontology(ontology).graph(graph).migrate();
            migrator.graph().commit();
        }
        catch (Throwable t) {
            t.printStackTrace(System.err);
            System.exit(-1);
        }		
	}
	
    @Test
    public void testSubpropertyInference() {
    	IRI createdProp = baseIri.resolve("#created");
    	Map<OWLNamedIndividual, Set<OWLNamedIndividual>> createdInstances = 
    			reasoner.getObjectPropertyInstances(manager.getOWLDataFactory().getOWLObjectProperty(createdProp));
    	int owlCount = createdInstances.values().stream().mapToInt(S -> S.size()).sum();
    	int mmCount = Graql.withGraph(migrator.graph()).match(Graql.var("r").isa(migrator.namer().objectPropertyName(createdProp)))
    		.stream().mapToInt(M -> 1).sum();
    	Assert.assertEquals(owlCount, mmCount);
    }	
}
