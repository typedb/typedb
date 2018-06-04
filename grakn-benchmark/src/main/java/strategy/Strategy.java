package strategy;

import java.util.Random;

public abstract class Strategy {
    private final double frequency;
    private final Random rand;

    public Strategy(double frequency, Random rand) {
        this.frequency = frequency;
        this.rand = rand;
    }

    public double getFrequency() {
        return frequency;
    }

    public Random getRand() {
        return rand;
    }
}
