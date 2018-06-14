package storage;

import java.util.Random;

public abstract class RandomConceptPicker {

    public static int generateRandomOffset(ConceptTypeCountStore conceptTypeCountStore, String typeLabel, Random rand){
        int typeCount = conceptTypeCountStore.get(typeLabel);
        return rand.nextInt(typeCount);
    }
}
