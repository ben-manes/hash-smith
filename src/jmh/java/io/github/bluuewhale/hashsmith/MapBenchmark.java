package io.github.bluuewhale.hashsmith;

import java.util.HashMap;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@Fork(
    value=1,
    jvmArgsAppend = {
        "--add-modules=jdk.incubator.vector",
        "--enable-preview",
//        "-Xms1g",
//        "-Xmx1g",
//        "-XX:+AlwaysPreTouch",
//        "-XX:+UnlockDiagnosticVMOptions",
//        "-XX:+DebugNonSafepoints",
//        "-XX:StartFlightRecording=name=JMHProfile,filename=jmh-profile.jfr,settings=profile",
//        "-XX:FlightRecorderOptions=stackdepth=256"
    }
)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class MapBenchmark {

	private static String randomUuidString(Random rnd) {
		return new UUID(rnd.nextLong(), rnd.nextLong()).toString();
	}

	@State(Scope.Benchmark)
	public static class ReadState {
        @Param({ "12000", "48000", "196000", "784000" }) // load factor equals to 74.x% (right before resizing)
		int size;

		SwissMap<String, Object> swiss;
		RobinHoodMap<String, Object> robin;
		Object2ObjectOpenHashMap<String, Object> fastutil;
		UnifiedMap<String, Object> unified;
		HashMap<String, Object> jdk;
		String[] keys;
		String[] misses;
		Random rnd;
		int nextKeyIndex;
		int nextMissIndex;
		Set<String> keySet;

		@Setup(Level.Trial)
		public void setup() {
			rnd = new Random(123);
			keys = new String[size];
			misses = new String[size];
			keySet = new java.util.HashSet<>(size * 2);
			for (int i = 0; i < size; i++) {
				var k = randomUuidString(rnd);
				keys[i] = k;
				keySet.add(k);
			}
			for (int i = 0; i < size; i++) {
				String miss;
				do { miss = randomUuidString(rnd); } while (keySet.contains(miss));
				misses[i] = miss;
			}
			nextKeyIndex = 0;
			nextMissIndex = 0;
			swiss = new SwissMap<>();
			robin = new RobinHoodMap<>();
			fastutil = new Object2ObjectOpenHashMap<>();
			unified = new UnifiedMap<>();
			jdk = new HashMap<>();
			for (int i = 0; i < size; i++) {
				swiss.put(keys[i], "dummy");
				robin.put(keys[i], "dummy");
				fastutil.put(keys[i], "dummy");
				unified.put(keys[i], "dummy");
				jdk.put(keys[i], "dummy");
			}
		}

		String nextHitKey() {
			var k = keys[nextKeyIndex];
			nextKeyIndex = (nextKeyIndex + 1) % keys.length;
			return k;
		}
		String nextMissingKey() {
			var k = misses[nextMissIndex];
			nextMissIndex = (nextMissIndex + 1) % misses.length;
			return k;
		}
	}

	@State(Scope.Thread)
	public static class MutateState {
        @Param({ "12000", "48000", "196000", "784000" }) // load factor equals to 74.x% (right before resizing)
		int size;

		String[] keys;
		String[] misses;
		Random rnd;
		int existingIndex;
		int missingIndex;
		SwissMap<String, Object> swiss;
		RobinHoodMap<String, Object> robin;
		Object2ObjectOpenHashMap<String, Object> fastutil;
		UnifiedMap<String, Object> unified;
		HashMap<String, Object> jdk;

		@Setup(Level.Trial)
		public void initKeys() {
			rnd = new Random(456);
			keys = IntStream.range(0, size).mapToObj(i -> randomUuidString(rnd)).toArray(String[]::new);
			misses = IntStream.range(0, size).mapToObj(i -> randomUuidString(rnd)).toArray(String[]::new);
		}

		@Setup(Level.Iteration)
		public void resetMaps() {
			swiss = new SwissMap<>();
			robin = new RobinHoodMap<>();
			fastutil = new Object2ObjectOpenHashMap<>();
			unified = new UnifiedMap<>();
			jdk = new HashMap<>();
			for (int i = 0; i < size; i++) {
				swiss.put(keys[i], "dummy");
				robin.put(keys[i], "dummy");
				fastutil.put(keys[i], "dummy");
				unified.put(keys[i], "dummy");
				jdk.put(keys[i], "dummy");
			}
			existingIndex = 0;
			missingIndex = 0;
		}

		String nextHitKey() {
			var k = keys[existingIndex];
			existingIndex = (existingIndex + 1) % keys.length;
			return k;
		}
		String nextMissingKey() {
			var k = misses[missingIndex];
			missingIndex = (missingIndex + 1) % misses.length;
			return k;
		}
		String nextValue() { return "dummy"; }
	}

	@State(Scope.Thread)
	public static class RemoveState {
		@Param({ "100", "1000", "10000" })
		int size;

		SwissMap<String, Object> swiss;
		RobinHoodMap<String, Object> robin;
		Object2ObjectOpenHashMap<String, Object> fastutil;
		UnifiedMap<String, Object> unified;
		HashMap<String, Object> jdk;
		String[] keys;
		String[] misses;
		Random rnd;

		@Setup(Level.Trial)
		public void initData() {
			rnd = new Random(789);
			keys = new String[size];
			misses = new String[size];
			var keySet = new java.util.HashSet<>(size * 2);
			for (int i = 0; i < size; i++) {
				var k = randomUuidString(rnd);
				keys[i] = k;
				keySet.add(k);
			}
			for (int i = 0; i < size; i++) {
				String miss;
				do { miss = randomUuidString(rnd); } while (keySet.contains(miss));
				misses[i] = miss;
			}
		}

		@Setup(Level.Invocation)
		public void resetMaps() {
			swiss = new SwissMap<>();
			robin = new RobinHoodMap<>();
			fastutil = new Object2ObjectOpenHashMap<>();
			unified = new UnifiedMap<>();
			jdk = new HashMap<>();
			for (int i = 0; i < size; i++) {
				var value = new Object();
				swiss.put(keys[i], value);
				robin.put(keys[i], value);
				fastutil.put(keys[i], value);
				unified.put(keys[i], value);
				jdk.put(keys[i], value);
			}
		}

		String hitKey() { return keys[rnd.nextInt(keys.length)]; }
		String missKey() { return misses[rnd.nextInt(misses.length)]; }
	}

	// ------- get hit/miss -------
	@Benchmark
	public void swissGetHit(ReadState s, Blackhole bh) {
		bh.consume(s.swiss.get(s.nextHitKey()));
	}

//	@Benchmark
	public void robinGetHit(ReadState s, Blackhole bh) {
        bh.consume(s.robin.get(s.nextHitKey()));
	}

	@Benchmark
	public void fastutilGetHit(ReadState s, Blackhole bh) {
        bh.consume(s.fastutil.get(s.nextHitKey()));
	}

	@Benchmark
	public void unifiedGetHit(ReadState s, Blackhole bh) {
        bh.consume(s.unified.get(s.nextHitKey()));
	}

	@Benchmark
	public void jdkGetHit(ReadState s, Blackhole bh) {
        bh.consume(s.jdk.get(s.nextHitKey()));
	}

	@Benchmark
	public void swissGetMiss(ReadState s, Blackhole bh) {
		bh.consume(s.swiss.get(s.nextMissingKey()));
	}

//	@Benchmark
	public void robinGetMiss(ReadState s, Blackhole bh) {
        bh.consume(s.robin.get(s.nextMissingKey()));
	}

	@Benchmark
	public void fastutilGetMiss(ReadState s, Blackhole bh) {
        bh.consume(s.fastutil.get(s.nextMissingKey()));
	}

	 @Benchmark
	public void unifiedGetMiss(ReadState s, Blackhole bh) {
        bh.consume(s.unified.get(s.nextMissingKey()));
	}

	@Benchmark
	public void jdkGetMiss(ReadState s, Blackhole bh) {
        bh.consume(s.jdk.get(s.nextMissingKey()));
	}

	// ------- mutating: put hit/miss -------
//	@Benchmark
	public void swissPutHit(MutateState s, Blackhole bh) {
        bh.consume(s.swiss.put(s.nextHitKey(), s.nextValue()));
	}

	//  @Benchmark
	public void robinPutHit(MutateState s, Blackhole bh) {
        bh.consume(s.robin.put(s.nextHitKey(), s.nextValue()));
	}

//  @Benchmark
	public void fastutilPutHit(MutateState s, Blackhole bh) {
        bh.consume(s.fastutil.put(s.nextHitKey(), s.nextValue()));
	}

	// @Benchmark
	public void unifiedPutHit(MutateState s, Blackhole bh) {
        bh.consume(s.unified.put(s.nextHitKey(), s.nextValue()));
	}

//	@Benchmark
	public void jdkPutHit(MutateState s, Blackhole bh) {
        bh.consume(s.jdk.put(s.nextHitKey(), s.nextValue()));
	}

//	@Benchmark
	public void swissPutMiss(MutateState s, Blackhole bh) {
        bh.consume(s.swiss.put(s.nextMissingKey(), s.nextValue()));
	}

	//  @Benchmark
	public void robinPutMiss(MutateState s, Blackhole bh) {
        bh.consume(s.robin.put(s.nextMissingKey(), s.nextValue()));
	}

// @Benchmark
	public void fastutilPutMiss(MutateState s, Blackhole bh) {
        bh.consume(s.fastutil.put(s.nextMissingKey(), s.nextValue()));
	}

	// @Benchmark
	public void unifiedPutMiss(MutateState s, Blackhole bh) {
        bh.consume(s.unified.put(s.nextMissingKey(), s.nextValue()));
	}

//	@Benchmark
	public void jdkPutMiss(MutateState s, Blackhole bh) {
        bh.consume(s.jdk.put(s.nextMissingKey(), s.nextValue()));
	}

	// ------- remove hit/miss -------
	// @Benchmark
	public void swissRemoveHit(RemoveState s, Blackhole bh) {
        bh.consume(s.swiss.remove(s.hitKey()));
	}

	// @Benchmark
	public void robinRemoveHit(RemoveState s, Blackhole bh) {
        bh.consume(s.robin.remove(s.hitKey()));
	}

	// @Benchmark
	public void fastutilRemoveHit(RemoveState s, Blackhole bh) {
        bh.consume(s.fastutil.remove(s.hitKey()));
	}

	// @Benchmark
	public void unifiedRemoveHit(RemoveState s, Blackhole bh) {
        bh.consume(s.unified.remove(s.hitKey()));
	}

	// @Benchmark
	public void jdkRemoveHit(RemoveState s, Blackhole bh) {
        bh.consume(s.jdk.remove(s.hitKey()));
	}

	// @Benchmark
	public void swissRemoveMiss(RemoveState s, Blackhole bh) {
        bh.consume(s.swiss.remove(s.missKey()));
	}

	// @Benchmark
	public void robinRemoveMiss(RemoveState s, Blackhole bh) {
        bh.consume(s.robin.remove(s.missKey()));
	}

	// @Benchmark
	public void fastutilRemoveMiss(RemoveState s, Blackhole bh) {
        bh.consume(s.fastutil.remove(s.missKey()));
	}

	// @Benchmark
	public void unifiedRemoveMiss(RemoveState s, Blackhole bh) {
        bh.consume(s.unified.remove(s.missKey()));
	}

	// @Benchmark
	public void jdkRemoveMiss(RemoveState s, Blackhole bh) {
        bh.consume(s.jdk.remove(s.missKey()));
	}

    // ------- iterate -------
//	@Benchmark
    public void swissIterate(ReadState s, Blackhole bh) {
        for (var e : s.swiss.entrySet()) {
            bh.consume(e.getKey());
            bh.consume(e.getValue());
        }
    }

    //	@Benchmark
    public void robinIterate(ReadState s, Blackhole bh) {
        for (var e : s.robin.entrySet()) {
            bh.consume(e.getKey());
            bh.consume(e.getValue());
        }
    }

    //	@Benchmark
    public void fastutilIterate(ReadState s, Blackhole bh) {
        for (var e : s.fastutil.object2ObjectEntrySet()) {
            bh.consume(e.getKey());
            bh.consume(e.getValue());
        }
    }

    // @Benchmark
    public void unifiedIterate(ReadState s, Blackhole bh) {
        for (var e : s.unified.entrySet()) {
            bh.consume(e.getKey());
            bh.consume(e.getValue());
        }
    }

    //	@Benchmark
    public void jdkIterate(ReadState s, Blackhole bh) {
        for (var e : s.jdk.entrySet()) {
            bh.consume(e.getKey());
            bh.consume(e.getValue());
        }
    }
}

