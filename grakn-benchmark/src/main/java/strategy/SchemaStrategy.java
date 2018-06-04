package strategy;

import java.util.Random;
import java.util.Set;

public class SchemaStrategy extends StrategyContainer<OperationStrategy>{


    public SchemaStrategy(double frequency, Random rand, Set<OperationStrategy> strategies) {
        super(frequency, rand, strategies);
    }

    public TypeStrategy getStrategy(){
        return pickStrategy().pickStrategy();
    }
}
