package storage;

import ai.grakn.concept.Concept;

import java.util.HashMap;

public class ConceptTypeCountStore implements ConceptStore {

    private final HashMap<String, Integer> conceptTypeCountStorage;

    public ConceptTypeCountStore() {
        this.conceptTypeCountStorage = new HashMap<>();
    }

    @Override
    public void add(Concept concept) {
        String typeLabel = concept.asThing().type().getLabel().toString();

//        Integer count = this.conceptTypeCountStorage.get(typeLabel);
//        this.conceptTypeCountStorage.put(typeLabel, count + 1);
        this.conceptTypeCountStorage.putIfAbsent(typeLabel, 0);
        this.conceptTypeCountStorage.compute(typeLabel, (k, v) -> v + 1);
    }

    public int get(String typeLabel) {
        return this.conceptTypeCountStorage.getOrDefault(typeLabel, 0);
    }

    public int total() {
        /*
        Return the total number of concepts
         */
        int total = 0;
        for (int c : conceptTypeCountStorage.values()) {
            total += c;
        }
        return total;
    }
}
