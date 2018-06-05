package strategy;

import java.util.Random;
import java.util.Set;

public abstract class Strategy {
    private final double frequency;
    private final Random rand;

    public Strategy(double frequency, Random rand) {
        this.frequency = frequency;
        this.rand = rand;
    }

//    public double getFrequency() {
//        return frequency;
//    }
//
//    public Random getRand() {
//        return rand;
//    }
//
//    @Override
//    public boolean equals(Object o) {
//        if (this == o) return true;
//        return o != null && getClass() == o.getClass();
//    }
//
//    @Override
//    public int hashCode() {
//        return Objects.hash(this.getClass());
//    }
}
