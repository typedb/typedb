package grakn.core.graph.core.util;

public class TestTimeAccumulator {

    static long totalTime = 0;
    static int times = 0;

    public static void addTime(long time) {
        totalTime += time;
        times += 1;
    }

    public static void reset(){
        totalTime = 0L;
    }

    public static long getTotalTimeInMs(){
        return totalTime/1000000;
    }

    public static int getTimes(){ return times; }
}
