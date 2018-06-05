package strategy;


import java.util.Random;
import java.util.Set;

//public class SchemaStrategy extends StrategyContainer<OperationStrategy>{
//
//
//    public SchemaStrategy(Set<OperationStrategy> strategies) {
//        super(strategies);
//    }
//
//    public TypeStrategy getStrategy(){
//        return pickStrategy().pickStrategy();
//    }
//}

public class SchemaStrategy {
    public final FrequencyOptionCollection<FrequencyOptionCollection> operationStrategies;

    public SchemaStrategy(FrequencyOptionCollection<FrequencyOptionCollection> operationStrategies) {
        this.operationStrategies = operationStrategies;
    }

    public TypeStrategy getStrategy(){
        return (TypeStrategy) this.operationStrategies.next().next();
    }
}
