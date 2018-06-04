package strategy;

import java.util.Random;
import java.util.Set;

public class OperationStrategy extends StrategyContainer<TypeStrategy>{

    public OperationStrategy(Set<TypeStrategy> typeStrategies, double frequency, Random rand) {
        super(frequency, rand, typeStrategies);
    }
}
