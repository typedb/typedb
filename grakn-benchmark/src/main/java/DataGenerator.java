import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.*;
import ai.grakn.graql.Match;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.admin.Answer;
import ai.grakn.remote.RemoteGrakn;
import ai.grakn.util.SimpleURI;
import strategy.DiscreteGaussianPDF;

import java.util.*;
import java.util.stream.Collectors;

import static ai.grakn.graql.internal.pattern.Patterns.var;

/*
Pseudo-code approach:
    Choose which type of concept is to be added
    If it's an entity, should this be connected to relationships or attributes upon being inserted?
        If so, how many connections should be made?
    If it's a relationship, it needs at least 1 role-player already present in the db (none causes erroneous behaviour)
        In this case, concepts that can fill the relationship's roles need to be selected
        (otherwise we'll be testing Grakn's validation the majority of the time, better to build invalid cases later)
            What procedure do we use to select them? Randomly?
        How many role-players should be selected?
    If it's an attribute, which concept should it belong to?
    ---

    New approach




    Joint probability distribution
    team-membership role-player probability distribution
    =======
    team-membership
    roles
    ----
    team        team-name           p(num(t) = n)   {   0.2 when n == 0
                                                        0.8 when n == 1
                                                        0 elsewhere }

    member      community-member    p(num(c) = n)   {
                employee

    =======
    has-name
    roles
    ----
    value       name                p(num(t) = n)   {   1 when n == 1   }
    owner       person              p(num(p) = n)   {
                company
*/

//public class QuantityGenerator {
//
//}


public class DataGenerator {
    public static final int RANDOM_SEED = 1;
    public static final int ADD_ENTITIES_OPERATION = 0;
    public static final int ADD_RELATIONSHIPS_OPERATION = 1;
    public static final int ADD_ATTRIBUTES_OPERATION = 2;
//    public static final int ADD_RELATIONSHIP_ROLEPLAYERS_OPERATION = 3; // Omitting this for now

    public static double ADD_ENTITIES_FREQUENCY = 0.3;
    public static double ADD_RELATIONSHIPS_FREQUENCY = 0.6;
    public static double ADD_ATTRIBUTES_FREQUENCY = 0.1;
//    public static final int ADD_RELATIONSHIP_ROLEPLAYERS_FREQUENCY = 3;

    //    public static HashMap<Integer, Double> freqs = new HashMap<Integer, Double>();
//    private TreeMap<Double,V> map;
    private NavigableMap<Double, Integer> frequencyOfConceptChoiceMap = new TreeMap<Double, Integer>();
    private RandomCollection<Integer> frequencyOfOperation;
    private RandomCollection<String> frequencyOfEntityType;

    private Random rand;
//    public List<Answer> entityTypes;
//    public List<EntityType> entityTypes;
    private ArrayList<Type> entityTypes = new ArrayList<Type>();
//    private ArrayList<strategy.DiscreteGaussianPDF> entityTypeDistributions = new ArrayList<strategy.DiscreteGaussianPDF>();
    private Hashtable<String, DiscreteGaussianPDF> entityTypeDistributions = new Hashtable<String, DiscreteGaussianPDF>();


    public DataGenerator() {

        this.rand = new Random(RANDOM_SEED);
        this.frequencyOfOperation = new RandomCollection<>(this.rand);
        this.frequencyOfOperation.add(DataGenerator.ADD_ENTITIES_FREQUENCY, DataGenerator.ADD_ENTITIES_OPERATION);
        this.frequencyOfOperation.add(DataGenerator.ADD_RELATIONSHIPS_FREQUENCY, DataGenerator.ADD_RELATIONSHIPS_OPERATION);
        this.frequencyOfOperation.add(DataGenerator.ADD_ATTRIBUTES_FREQUENCY, DataGenerator.ADD_ATTRIBUTES_OPERATION);

        this.frequencyOfEntityType = new RandomCollection<>((this.rand));
        this.frequencyOfEntityType.add(0.4, "occupation");
        this.frequencyOfEntityType.add(0.6, "person");

//        this.frequencyOfEntityType.add(0.1, "societal-role");
//        this.frequencyOfEntityType.add(0.1, "vocation");

        this.entityTypeDistributions.put("person", new DiscreteGaussianPDF(this.rand, 5.0, 1.0));
        this.entityTypeDistributions.put("company", new DiscreteGaussianPDF(this.rand, 5.0, 1.0));

        GraknSession session = RemoteGrakn.session(new SimpleURI("localhost:48555"), Keyspace.of("societal_model"));
        try (GraknTx tx = session.open(GraknTxType.READ)) {
            QueryBuilder qb = tx.graql();
            Match match = qb.match(var("x").isa("person").has("forename", "Natalie")).limit(50);
            List<Answer> result = match.get().execute();
            Concept concept = result.iterator().next().get("x");
            EntityType type = concept.asEntity().type();

            List<Answer> plays = qb.match(var("x").isa(type.getLabel().toString()).plays(var("r"))).get().execute();
            System.out.print("hey");

//            Explicit relationships only
            List<Label> explicitRelationships = type.plays()
                    .filter(p -> !p.isImplicit())
                    .map(SchemaConcept::getLabel)
                    .collect(Collectors.toList());

//            All relationships explicit and implicit
            List<Label> c = type.plays().map(SchemaConcept::getLabel).collect(Collectors.toList());
        }
    }

    private int chooseOperation() {
//        double rnd = this.rand.nextDouble();
//        return this.frequencyOfConceptChoiceMap.ceilingEntry(rnd).getValue();
        return this.frequencyOfOperation.next();
    }

    private <T extends Type> ArrayList<T> getTypes(GraknTx tx, String typeName){
//    private ArrayList<Type> getTypes(GraknTx tx, String typeName){
        ArrayList<T> conceptTypes = new ArrayList<T>();
        QueryBuilder qb = tx.graql();
        Match match = qb.match(var("x").sub(typeName));
        List<Answer> result = match.get().execute();
        T conceptType;

        // TODO This instead?
        // this.conceptTypes.add(result.iterator().forEachRemaining(get("x").asEntityType()));
        Iterator<Answer> conceptTypeIterator = result.iterator();
        while (conceptTypeIterator.hasNext()) {
            conceptType = (T) conceptTypeIterator.next().get("x");
            conceptTypes.add(conceptType);
        }
        conceptTypes.remove(0);  // Remove type "entity"

        return conceptTypes;
    }

//    private List<Answer> matchConceptTypes(GraknTx tx, String typeName){
//        QueryBuilder qb = tx.graql();
//        Match match = qb.match(var("x").sub(typeName));
//        List<Answer> result = match.get().execute();
//        return result;
//    }
//
//
//    private ArrayList<EntityType> getEntityTypes(GraknTx tx, String typeName){
//        ArrayList<EntityType> conceptTypes = new ArrayList<EntityType>();
//        EntityType conceptType;
//
//
//        QueryBuilder qb = tx.graql();
//        Match match = qb.match(var("x").sub(typeName));
//        List<Answer> result = match.get().execute();
//
//        // TODO This instead?
//        // this.conceptTypes.add(result.iterator().forEachRemaining(get("x").asEntityType()));
//        Iterator<Answer> conceptTypeIterator = result.iterator();
//        while (conceptTypeIterator.hasNext()) {
//            conceptType = conceptTypeIterator.next().get("x").asEntityType();
//            conceptTypes.add(conceptType);
//        }
//        conceptTypes.remove(0);  // Remove type "entity"
//
//        return conceptTypes;
//    }

//    private int numFromDistribution(PDF distribution){
//        return 5;
//    }

    public void generate() {
        int max_iterations = 100;
        int it = 0;
        int op;

        // Find all entity types
        GraknSession session = RemoteGrakn.session(new SimpleURI("localhost:48555"), Keyspace.of("societal_model"));
        try (GraknTx tx = session.open(GraknTxType.READ)) {
            // TODO This works, but now entityTypes Types not EntityTypes
            this.entityTypes = this.getTypes(tx, "entity");
        }

        String entityTypeLabel;

//        while (it < max_iterations) {
//            op = this.chooseOperation();
//            System.out.print(Integer.toString(op) + "\n");
//            strategy.DiscreteGaussianPDF entityTypeDistribution;
//
//            if (op == ADD_ENTITIES_OPERATION) {
//                entityTypeLabel = this.frequencyOfEntityType.next();
//                entityTypeDistribution = this.entityTypeDistributions.get(entityTypeLabel);
//                int numEntities = entityTypeDistribution.next();
////                this.spawnEntities(entityTypeLabel, numEntities);
//
//
//            } else if (op == ADD_RELATIONSHIPS_OPERATION) {
//
//            } else if (op == ADD_ATTRIBUTES_OPERATION) {
//
//            } else {
//                throw new RuntimeException("Encountered a scenario that should not occur. One operation should always be picked.");
//            }
//
//            it++;
//        }
        while (it < max_iterations) {

//            operationStrategy = schemaStrategy.pickStrategy();
//            operationStrategy.
            it++;
        }
    }

    public static void main(String[] args) {
        System.out.print("hello grakn");
        DataGenerator dg = new DataGenerator();
        dg.generate();
    }
}
