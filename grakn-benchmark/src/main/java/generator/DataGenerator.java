/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package generator;

import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.client.Grakn;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.Query;
import ai.grakn.graql.answer.ConceptMap;
import ai.grakn.util.SimpleURI;
import pdf.ConstantPDF;
import pdf.DiscreteGaussianPDF;
import pdf.UniformPDF;
import pick.CentralStreamProvider;
import pick.FromIdStoragePicker;
import pick.IntegerPicker;
import pick.NotInRelationshipConceptIdStream;
import pick.PickableCollectionValuePicker;
import pick.StreamProvider;
import storage.ConceptStore;
import storage.IdStoreInterface;
import storage.IgniteConceptIdStore;
import storage.InsertionAnalysis;
import storage.SchemaManager;
import strategy.AttributeOwnerTypeStrategy;
import strategy.AttributeStrategy;
import strategy.EntityStrategy;
import strategy.RelationshipStrategy;
import strategy.RolePlayerTypeStrategy;
import strategy.RouletteWheelCollection;
import strategy.TypeStrategyInterface;

import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Stream;

/**
 *
 */
public class DataGenerator {

    private final String uri;
    private final String keyspace;
    private static String schemaRelativeDirPath = "/grakn-benchmark/src/main/resources/societal_model.gql";

    public static final int RANDOM_SEED = 1;
    private int iteration = 0;

    private Random rand;
    private HashSet<EntityType> entityTypes;
    private HashSet<RelationshipType> relationshipTypes;
    private HashSet<AttributeType> attributeTypes;
    private HashSet<Role> roles;


    private RouletteWheelCollection<TypeStrategyInterface> entityStrategies;
    private RouletteWheelCollection<TypeStrategyInterface> relationshipStrategies;
    private RouletteWheelCollection<TypeStrategyInterface> attributeStrategies;

    private RouletteWheelCollection<RouletteWheelCollection<TypeStrategyInterface>> operationStrategies;

    private ConceptStore storage;

    public DataGenerator(String keyspace, String uri) {
        this.keyspace = keyspace;
        this.uri = uri;
        this.reset();
        this.rand = new Random(RANDOM_SEED);
        entityStrategies = new RouletteWheelCollection<>(this.rand);
        relationshipStrategies = new RouletteWheelCollection<>(this.rand);
        attributeStrategies = new RouletteWheelCollection<>(this.rand);
        operationStrategies = new RouletteWheelCollection<>(this.rand);

        Grakn.Session session = this.getSession();

        try (GraknTx tx = session.transaction(GraknTxType.READ)) {

            // TODO Add checking to ensure that all of these strategies make sense
            this.entityTypes = SchemaManager.getTypesOfMetaType(tx, "entity");
            this.relationshipTypes = SchemaManager.getTypesOfMetaType(tx, "relationship");
            this.attributeTypes = SchemaManager.getTypesOfMetaType(tx, "attribute");
            this.roles = SchemaManager.getRoles(tx, "role");

            this.storage = new IgniteConceptIdStore(this.entityTypes, this.relationshipTypes, this.attributeTypes);

            this.entityStrategies.add(
                    0.5,
                    new EntityStrategy(
                            SchemaManager.getTypeFromString("person", this.entityTypes),
                            new UniformPDF(this.rand, 20, 40)
                    ));

            this.entityStrategies.add(
                    0.5,
                    new EntityStrategy(
                            SchemaManager.getTypeFromString("company", this.entityTypes),
                            new UniformPDF(this.rand, 1, 5)
                    )
            );

            Set<RolePlayerTypeStrategy> employmentRoleStrategies = new HashSet<RolePlayerTypeStrategy>();

            employmentRoleStrategies.add(
                    new RolePlayerTypeStrategy(
                            SchemaManager.getTypeFromString("employee", this.roles),
                            SchemaManager.getTypeFromString("person", this.entityTypes),
                            new ConstantPDF(1),
                            new StreamProvider<>(
                                    new FromIdStoragePicker<>(
                                            this.rand,
                                            (IdStoreInterface) this.storage,
                                            "person",
                                            ConceptId.class)
                            )
                    )
            );

            employmentRoleStrategies.add(
                    new RolePlayerTypeStrategy(
                            SchemaManager.getTypeFromString("employer", this.roles),
                            SchemaManager.getTypeFromString("company", this.entityTypes),
                            new ConstantPDF(1),
                            new CentralStreamProvider<>(
                                    new NotInRelationshipConceptIdStream(
                                            "employment",
                                            "employer",
                                            100,
                                            new FromIdStoragePicker<>(
                                                    this.rand,
                                                    (IdStoreInterface) this.storage,
                                                    "company",
                                                    ConceptId.class)
                                    )
                            )
                    )
            );

            this.relationshipStrategies.add(
                    0.3,
                    new RelationshipStrategy(
                            SchemaManager.getTypeFromString("employment", this.relationshipTypes),
                            new DiscreteGaussianPDF(this.rand, 30.0, 30.0),
                            employmentRoleStrategies)
            );

            RouletteWheelCollection<String> nameValueOptions = new RouletteWheelCollection<String>(this.rand)
            .add(0.5, "Da Vinci")
            .add(0.5, "Nero")
            .add(0.5, "Grakn")
            .add(0.5, "Google")
            .add(0.5, "Facebook")
            .add(0.5, "Microsoft")
            .add(0.5, "JetBrains")
            .add(0.5, "IBM")
            .add(0.5, "Starbucks");

//            TODO How to get the datatype without having to declare it? Does it make sense to do this?
//            SchemaManager.getDatatype("company", this.entityTypes),

            this.attributeStrategies.add(
                    1.0,
                    new AttributeStrategy<>(
                            SchemaManager.getTypeFromString("name", this.attributeTypes),
                            new UniformPDF(this.rand, 3, 100),
                            new AttributeOwnerTypeStrategy<>(
                                    SchemaManager.getTypeFromString("company", this.entityTypes),
                                    new StreamProvider<>(
                                            new FromIdStoragePicker<>(
                                                    this.rand,
                                                    (IdStoreInterface) this.storage,
                                                    "company",
                                                    ConceptId.class)
                                    )
                            ),
                            new StreamProvider<>(
                                    new PickableCollectionValuePicker<String>(nameValueOptions)
                            )
                    )
            );


//            RouletteWheelCollection<String> genderValueOptions = new RouletteWheelCollection<String>(this.rand)
//            .add(0.5, "male")
//            .add(0.5, "female");
//
//
//            this.attributeStrategies.add(
//                    1.0,
//                    new AttributeStrategy<String>(
//                            SchemaManager.getTypeFromString("gender", this.attributeTypes),
//                            new UniformPDF(this.rand, 3, 20),
//                            new AttributeOwnerTypeStrategy<>(
//                                    SchemaManager.getTypeFromString("name", this.attributeTypes),
//                                    new StreamProvider<>(
//                                            new FromIdStoragePicker<>(
//                                                    this.rand,
//                                                    (IdStoreInterface) this.storage,
//                                                    "name",
//                                                    String.class)
//                                    )
//                            ),
//                            new StreamProvider<>(
//                                    new PickableCollectionValuePicker<String>(genderValueOptions)
//                            )
//                    )
//            );

//            RouletteWheelCollection<Integer> ratingValueOptions = new RouletteWheelCollection<Integer>(this.rand)
//            .add(0.5, 1)
//            .add(0.5, 2)
//            .add(0.5, 3)
//            .add(0.5, 4)
//            .add(0.5, 5)
//            .add(0.5, 6)
//            .add(0.5, 7)
//            .add(0.5, 8)
//            .add(0.5, 9)
//            .add(0.5, 10);


            this.attributeStrategies.add(
                    1.0,
                    new AttributeStrategy<>(
                            SchemaManager.getTypeFromString("rating", this.attributeTypes),
                            new UniformPDF(this.rand, 10, 20),
                            new AttributeOwnerTypeStrategy<>(
                                    SchemaManager.getTypeFromString("name", this.attributeTypes),
                                    new StreamProvider<>(
                                            new FromIdStoragePicker<>(
                                                    this.rand,
                                                    (IdStoreInterface) this.storage,
                                                    "name",
                                                    String.class)
                                    )
                            ),
                            new StreamProvider<>(
                                    new IntegerPicker(this.rand, 0, 100)
                            )
                    )
            );


            this.attributeStrategies.add(
                    5.0,
                    new AttributeStrategy<>(
                            SchemaManager.getTypeFromString("rating", this.attributeTypes),
                            new UniformPDF(this.rand, 3, 40),
                            new AttributeOwnerTypeStrategy<>(
                                    SchemaManager.getTypeFromString("company", this.entityTypes),
                                    new StreamProvider<>(
                                            new FromIdStoragePicker<>(
                                                    this.rand,
                                                    (IdStoreInterface) this.storage,
                                                    "company",
                                                    ConceptId.class)
                                    )
                            ),
                            new StreamProvider<>(
                                    new IntegerPicker(this.rand, 0, 1000000)
                            )
                    )
            );


            this.attributeStrategies.add(
                    3.0,
                    new AttributeStrategy<>(
                            SchemaManager.getTypeFromString("rating", this.attributeTypes),
                            new UniformPDF(this.rand, 40, 60),
                            new AttributeOwnerTypeStrategy<>(
                                    SchemaManager.getTypeFromString("employment", this.relationshipTypes),  //TODO change this so that declaring the MetaType to search isn't necessary
                                    new StreamProvider<>(
                                            new FromIdStoragePicker<>(
                                                    this.rand,
                                                    (IdStoreInterface) this.storage,
                                                    "employment",
                                                    ConceptId.class)
                                    )
                            ),
                            new StreamProvider<>(
                                    new IntegerPicker(this.rand, 1, 10)
                            )
                    )
            );
        }

        this.operationStrategies.add(0.6, this.entityStrategies);
        this.operationStrategies.add(0.2, this.relationshipStrategies);
        this.operationStrategies.add(0.2, this.attributeStrategies);
    }

    private Grakn.Session getSession() {
        return (new Grakn(new SimpleURI((uri)))).session(Keyspace.of(keyspace));
    }

    public void generate(int numConceptsLimit) {
        /*
        This method can be called multiple times, with a higher numConceptsLimit each time, so that the generation can be
        effectively paused while benchmarking takes place
        */

        Grakn.Session session = this.getSession();

        GeneratorFactory gf = new GeneratorFactory();
        int conceptTotal = this.storage.total();

        while (conceptTotal < numConceptsLimit) {
            System.out.printf("---- Iteration %d ----\n", this.iteration);
            try (GraknTx tx = session.transaction(GraknTxType.WRITE)) {

                //TODO Deal with this being an Object. TypeStrategy should be/have an interface for this purpose?
                TypeStrategyInterface typeStrategy = this.operationStrategies.next().next();
                System.out.print("Generating instances of concept type \"" + typeStrategy.getTypeLabel() + "\"\n");

                GeneratorInterface generator = gf.create(typeStrategy, tx); // TODO Can we do without creating a new generator each iteration

                System.out.println("Using generator " + generator.getClass().toString());
                Stream<Query> queryStream = generator.generate();
                
                this.processQueryStream(queryStream);

                iteration++;
                conceptTotal = this.storage.total();
                System.out.printf(String.format("---- %d concepts ----\n", conceptTotal), this.iteration);
                tx.commit();
            }
        }
    }

    private void processQueryStream(Stream<Query> queryStream) {
        /*
        Make the data insertions from the stream of queries generated
         */
        queryStream.map(q -> (InsertQuery) q)
                .forEach(q -> {
                    List<ConceptMap> insertions = q.execute();
                    insertions.forEach(insert -> {
                        HashSet<Concept> insertedConcepts = InsertionAnalysis.getInsertedConcepts(q, insertions);
                        if (insertedConcepts.isEmpty()) {
                            throw new RuntimeException("No concepts were inserted");
                        }
                        insertedConcepts.forEach(concept -> this.storage.add(concept));
                    });
                });
    }

    public void reset() {
        System.out.println("Initialising keyspace...");
        SchemaManager.initialise(this.getSession(), schemaRelativeDirPath);
        System.out.println("done");
        this.iteration = 0;
    }

    public static void main(String[] args) {
        String uri = "localhost:48555";
        String keyspace = "societal_model";
        DataGenerator dg = new DataGenerator(keyspace, uri);
        System.out.print("Generating data...\n");

        long startTime = System.nanoTime();
        dg.generate(100);
        dg.generate(200);
        dg.generate(300);
        dg.generate(400);
//        dg.generate(1000);
//        dg.generate(10000);
//        dg.generate(1000000);//Started at 19:33
        long endTime = System.nanoTime();
        long duration = (endTime - startTime) / 1000000000;

        long hours = duration / 3600;
        long minutes = (duration % 3600) / 60;
        long seconds = duration % 60;

        String timeString = String.format("%02d:%02d:%02d", hours, minutes, seconds);

        System.out.printf("Generation took %s\n", timeString);
        System.out.print("Done\n");
    }
}
