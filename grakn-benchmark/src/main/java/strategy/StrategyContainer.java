package strategy;

import java.util.Random;
import java.util.Set;

public abstract class StrategyContainer<T extends Strategy> extends Strategy{
    private final Set<T> strategies;

    public StrategyContainer(double frequency, Random rand, Set<T> strategies) {
        super(frequency, rand);
        this.strategies = strategies;
    }

    public T pickStrategy(){
        return null;
    }
}
