package test.io.mindmaps.migration.owl;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.semanticweb.HermiT.Reasoner;
import org.semanticweb.HermiT.Configuration;
import org.semanticweb.owlapi.model.OWLOntology;

@Ignore
public class TestReasoning extends TestOwlMindMapsBase {

	private Reasoner reasoner = null;
	
	@Before
	public void loadShakespeare() {
        try {
            OWLOntology O = loadOntologyFromResource("/io/mindmaps/migration/owl/samples/shakespeare.owl");   
            reasoner = new Reasoner(new Configuration(), O);
            migrator.ontology(O).graph(graph).migrate();
            migrator.graph().commit();
        }
        catch (Throwable t) {
            t.printStackTrace(System.err);
            System.exit(-1);
        }		
	}
	
    @Test
    public void testSubpropertyInference() {
    	
    }	
}
