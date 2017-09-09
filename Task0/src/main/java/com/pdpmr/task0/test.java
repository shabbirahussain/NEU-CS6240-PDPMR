package com.pdpmr.task0;

        import com.pdpmr.task0.subtasks.LetterScorer;
        import org.openjdk.jmh.annotations.Benchmark;
        import org.openjdk.jmh.annotations.BenchmarkMode;
        import org.openjdk.jmh.annotations.Fork;
        import org.openjdk.jmh.annotations.Measurement;
        import org.openjdk.jmh.annotations.Mode;
        import org.openjdk.jmh.annotations.OutputTimeUnit;
        import org.openjdk.jmh.annotations.Param;
        import org.openjdk.jmh.annotations.Scope;
        import org.openjdk.jmh.annotations.State;
        import org.openjdk.jmh.annotations.Warmup;
        import org.openjdk.jmh.runner.Runner;
        import org.openjdk.jmh.runner.RunnerException;
        import org.openjdk.jmh.runner.options.Options;
        import org.openjdk.jmh.runner.options.OptionsBuilder;

        import java.io.IOException;
        import java.math.BigInteger;
        import java.util.Map;
        import java.util.Properties;
        import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Benchmark)
public class test {

    /**
     * In many cases, the experiments require walking the configuration space
     * for a benchmark. This is needed for additional control, or investigating
     * how the workload performance changes with different settings.
     */

    @Param({"1", "31", "65", "101", "103"})
    public int arg;

    @Param({"1", "2", "3"})
    public int maxThreads;

    public int kNeighborhood;
    public String dataDir, validCharRegex;

    public test(){
        try {
            Properties prop = new Properties();
            prop.load(Main.class.getClassLoader().getResourceAsStream("config.properties"));

            //maxThreads = Integer.parseInt(prop.getProperty("max-threads"));
            dataDir = prop.getProperty("data-dir");
            validCharRegex = prop.getProperty("valid-char-regex");
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    public Map<Character, Integer> subtask1()
            throws IOException {
        return (new LetterScorer(maxThreads, validCharRegex))
                .getScoreFromCorpus(dataDir);
    }

    @Benchmark
    public boolean bench() {
        return BigInteger.valueOf(arg).isProbablePrime(1);
    }


    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(test.class.getSimpleName())
//                .param("arg", "41", "42") // Use this to selectively constrain/override parameters
                .build();

        new Runner(opt).run();
    }

}