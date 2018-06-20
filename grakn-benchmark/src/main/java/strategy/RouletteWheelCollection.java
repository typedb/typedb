package strategy;

import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;

//TODO Shouldn't have name including "Collection"?
public class RouletteWheelCollection<T> implements PickableCollection {
    private final NavigableMap<Double, T> map = new TreeMap<Double, T>();
    private final Random random;
    private double total = 0;

    public RouletteWheelCollection(Random random) {
        this.random = random;
    }

    public RouletteWheelCollection<T> add(double weight, T result) {
        if (weight <= 0) return this;
        total += weight;
        map.put(total, result);
        return this;
    }

    public T next() {
        double value = random.nextDouble() * total;
        return map.higherEntry(value).getValue();
    }
}