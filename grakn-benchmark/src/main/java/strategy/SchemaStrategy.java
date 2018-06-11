package strategy;

public class SchemaStrategy {
    public final FrequencyOptionCollection<FrequencyOptionCollection> operationStrategies;

    public SchemaStrategy(FrequencyOptionCollection<FrequencyOptionCollection> operationStrategies) {
        this.operationStrategies = operationStrategies;
    }

    public TypeStrategy getStrategy(){
        return (TypeStrategy) this.operationStrategies.next().next();
    }
}
