package test.io.grakn.migration.owl;

import ai.grakn.exception.GraknValidationException;
import ai.grakn.graql.Graql;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.semanticweb.HermiT.Configuration;
import org.semanticweb.HermiT.Reasoner;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLNamedIndividual;
import org.semanticweb.owlapi.model.OWLOntology;

import java.util.Map;
import java.util.Set;

public class TestSubProperties extends TestOwlGraknBase {
	private IRI baseIri = IRI.create("http://www.workingontologist.org/Examples/Chapter3/shakespeare.owl");
	private OWLOntology shakespeare = null;
	
	@Before
	public void loadShakespeare() throws GraknValidationException {
        shakespeare = loadOntologyFromResource("/io/grakn/migration/owl/samples/shakespeare.owl");
        migrator.ontology(shakespeare).graph(graph).migrate();
        migrator.graph().commit();
	}
	
    @Test
    public void testSubpropertyInference() {
    	Reasoner reasoner = new Reasoner(new Configuration(), shakespeare);
    	IRI createdProp = baseIri.resolve("#created");
    	Map<OWLNamedIndividual, Set<OWLNamedIndividual>> createdInstances = 
    			reasoner.getObjectPropertyInstances(manager.getOWLDataFactory().getOWLObjectProperty(createdProp));
    	int owlCount = createdInstances.values().stream().mapToInt(S -> S.size()).sum();
    	int mmCount = Graql.withGraph(migrator.graph()).match(Graql.var("r").isa(migrator.namer().objectPropertyName(createdProp)))
    		.stream().mapToInt(M -> 1).sum();
    	Assert.assertEquals(owlCount, mmCount);
    }
}
