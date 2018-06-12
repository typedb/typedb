import ai.grakn.*;
import ai.grakn.concept.*;
import ai.grakn.graql.*;
import ai.grakn.graql.admin.Answer;
import ai.grakn.remote.RemoteGrakn;
import ai.grakn.util.SimpleURI;
import generator.Generator;
import generator.GeneratorFactory;
import pdf.DiscreteGaussianPDF;
import storage.ConceptIdStore;
import storage.ConceptPicker;
import storage.ConceptStore;
import storage.RandomConceptIdPicker;
import strategy.*;

import java.util.*;
import java.util.stream.Stream;

import static ai.grakn.graql.internal.pattern.Patterns.var;

public class DataGenerator {

    private static String uri = "localhost:48555";
    private static String keyspace = "societal_model";

    public static final int RANDOM_SEED = 1;

    private Random rand;
    private ArrayList<EntityType> entityTypes;
    private ArrayList<RelationshipType> relationshipTypes;
    private ArrayList<AttributeType> attributeTypes;
    private ArrayList<Role> roles;

//    private Set<EntityStrategy> entityStrategies = new HashSet<EntityStrategy>();
//    private Set<RelationshipStrategy> relationshipStrategies = new HashSet<RelationshipStrategy>();
//    private Set<OperationStrategy>operationStrategies = new HashSet<OperationStrategy>();
//    private SchemaStrategy schemaStrategy;

    private FrequencyOptionCollection<EntityStrategy> entityStrategies;
    private FrequencyOptionCollection<RelationshipStrategy> relationshipStrategies;
    private FrequencyOptionCollection<AttributeStrategy> attributeStrategies;

//    private FrequencyOptionCollection<FrequencyOptionCollection<TypeStrategy>> operationStrategies;
    private FrequencyOptionCollection<FrequencyOptionCollection> operationStrategies;

    private boolean doExecution = true;
    private ConceptStore conceptIdStore;
    private RandomConceptIdPicker conceptPicker;

    public DataGenerator() {

        this.rand = new Random(RANDOM_SEED);
        entityStrategies = new FrequencyOptionCollection<EntityStrategy>(this.rand);
        relationshipStrategies = new FrequencyOptionCollection<RelationshipStrategy>(this.rand);
        attributeStrategies = new FrequencyOptionCollection<AttributeStrategy>(this.rand);
//        operationStrategies = new FrequencyOptionCollection<FrequencyOptionCollection<TypeStrategy>>(this.rand);
        operationStrategies = new FrequencyOptionCollection<FrequencyOptionCollection>(this.rand);

        GraknSession session = this.getSession();

        try (GraknTx tx = session.open(GraknTxType.READ)) {
            this.entityTypes = this.getTypes(tx, "entity");
            this.relationshipTypes = this.getTypes(tx, "relationship");
            this.attributeTypes = this.getTypes(tx, "attribute");
            this.roles = this.getTypes(tx, "role");


            this.entityStrategies.add(
                    0.5,
                    new EntityStrategy(
                            this.getTypeFromString("person", this.entityTypes),
                            new DiscreteGaussianPDF(this.rand, 10.0, 2.0)));

            this.entityStrategies.add(
                    0.5,
                    new EntityStrategy(
                            this.getTypeFromString("occupation", this.entityTypes),
                            new DiscreteGaussianPDF(this.rand, 2.0, 1.0)));

            Set<RelationshipRoleStrategy> employmentRoleStrategies = new HashSet<RelationshipRoleStrategy>();

            employmentRoleStrategies.add(
                    new RelationshipRoleStrategy(
                            this.getTypeFromString("employee", this.roles),
                            new RolePlayerTypeStrategy(
                                    this.getTypeFromString("person", this.entityTypes),
                                    new DiscreteGaussianPDF(this.rand, 20.0, 2.0),
                                    new RandomConceptIdPicker(this.rand, false))
                    )
            );

//            employmentRoleStrategies.add(
//                    new RelationshipRoleStrategy(
//                            this.getTypeFromString("profession", this.roles),
//                            new RolePlayerTypeStrategy(
//                                    this.getTypeFromString("occupation", this.entityTypes),
//                                    new DiscreteGaussianPDF(this.rand, 2.0, 1.0),
//                                    new RandomConceptIdPicker(this.rand, false))
//                    )
//            );

            this.relationshipStrategies.add(
                    0.3,
                    new RelationshipStrategy(
                            this.getTypeFromString("employment", this.relationshipTypes),
                            new DiscreteGaussianPDF(this.rand, 2.0, 1.0),
                            employmentRoleStrategies)
            );
        }

        this.operationStrategies.add(0.8, this.entityStrategies);
        this.operationStrategies.add(0.2, this.relationshipStrategies);
        this.operationStrategies.add(0.0, this.attributeStrategies);

    }

    private GraknSession getSession(){
        return RemoteGrakn.session(new SimpleURI(uri), Keyspace.of(keyspace));
    }

    private <T extends SchemaConcept> T getTypeFromString(String typeName, ArrayList<T> typeInstances) {
        Iterator iter = typeInstances.iterator();
        String l;
        T currentType;

        while (iter.hasNext()) {
            currentType = (T) iter.next();
            l = currentType.getLabel().toString();
            if (l.equals(typeName)){
                return currentType;
            }
        }
        throw new RuntimeException("Couldn't find a concept type with name \"" + typeName + "\"");
    }

    private <T extends SchemaConcept> ArrayList<T> getTypes(GraknTx tx, String conceptTypeName) {
        ArrayList<T> conceptTypes = new ArrayList<T>();
        QueryBuilder qb = tx.graql();
        Match match = qb.match(var("x").sub(conceptTypeName));
        List<Answer> result = match.get().execute();
        T conceptType;

        // TODO This instead?
        // this.conceptTypes.add(result.iterator().forEachRemaining(get("x").asEntityType()));
        Iterator<Answer> conceptTypeIterator = result.iterator();
        while (conceptTypeIterator.hasNext()) {
//            conceptType = (T) conceptTypeIterator.next().get("x").asType();
            conceptType = (T) conceptTypeIterator.next().get("x");
            conceptTypes.add(conceptType);
        }
        conceptTypes.remove(0);  // Remove type "entity"

        return conceptTypes;
    }

    public void generate() {
        int max_iterations = 100;
        int it = 0;

        // Store the ids of the concepts inserted by type, using type as the key
//        this.conceptIdStore = new HashMap<String, ArrayList<String>>();
        this.conceptIdStore = new ConceptIdStore();
        this.conceptPicker = new RandomConceptIdPicker(this.rand, false);

        GraknSession session = this.getSession();


            GeneratorFactory gf = new GeneratorFactory();

            while (it < max_iterations) {
                try (GraknTx tx = session.open(GraknTxType.WRITE)) {
                Generator generator = gf.create(this.operationStrategies.next().next(), tx, this.conceptIdStore, this.conceptPicker);

                Stream<Query> queriesStream = generator.generate();

                if (this.doExecution) {
                    queriesStream.map(q -> (InsertQuery) q)
                            .forEachOrdered(q -> {
                        List<Answer> insertions = q.execute();
                        insertions.forEach(insert -> insert.concepts().forEach(concept -> {
                            this.conceptIdStore.add(concept);
                        }));
                    });
                } else {

                    // Print the queries rather than executing
                    Iterator<Query> iter = queriesStream.iterator();

                    while (iter.hasNext()) {
                        String s = iter.next().toString();
                        System.out.print(s + "\n");
                    }
                }
                it++;
                if (this.doExecution) {
                    tx.commit();
                }
            }

        }
    }

    public static void main(String[] args) {
        System.out.print("hello grakn\n");
        DataGenerator dg = new DataGenerator();
        System.out.print("Generating data...\n");
        dg.generate();
        System.out.print("Done\n");
    }
}
