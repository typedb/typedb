//package storage;
//
//import java.util.*;
//import java.util.stream.IntStream;
//import java.util.stream.Stream;
//
//public class RandomConceptIdPicker implements ConceptPicker {
//
//    //    ConceptIdStore conceptIdStore;
//    private final Boolean pickUniquely;
//    Random rand;
//
////    public RandomConceptIdPicker(ConceptIdStore conceptIdStore, Boolean pickUniquely, Random rand) {
////        this.conceptIdStore = conceptIdStore;
////        this.pickUniquely = pickUniquely;
////        this.rand = rand;
////    }
//
//    public RandomConceptIdPicker(Random rand, Boolean pickUniquely) {
//        this.pickUniquely = pickUniquely;
//        this.rand = rand;
//    }
//
//    @Override
//    public Stream<String> get(String typeLabel, ConceptStore conceptStore, int numConcepts) {
//        if (conceptStore.get(typeLabel).size() >= numConcepts){
//            return this.createConceptIdStream(typeLabel, conceptStore, numConcepts);
//        }
//        return null;
//    }
//
//    @Override
//    public void reset(){}
//
//    private Stream<String> createConceptIdStream(String typeLabel, ConceptStore conceptStore, int numConcepts){
//        HashSet<Integer> usedIndices = new HashSet<Integer>();
//        ArrayList<String> typeIds = conceptStore.get(typeLabel);
//        IntStream randomIntStream = this.rand.ints(0, typeIds.size());
//
//        Iterator<Integer> iter = randomIntStream.iterator();
//
//        Stream<String> conceptIdStream = Stream.generate(() -> {
//            Integer randomInt = null;
//            while (randomInt == null) {
//                randomInt = iter.next();
//                if (usedIndices.contains(randomInt)) {
//                    randomInt = null;
//                }
//            }
//            return typeIds.get(randomInt);
//        }).limit(numConcepts);
//
//        // TODO actually more complicated: need to create a list/stream of ids picked from the list without replacement
//        return conceptIdStream;
//    }
//
//}
