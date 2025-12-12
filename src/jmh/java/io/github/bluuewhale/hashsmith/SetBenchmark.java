package io.github.bluuewhale.hashsmith;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@Fork(2)
@Warmup(iterations = 3)
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class SetBenchmark {

	@State(Scope.Benchmark)
	public static class ReadState {
		@Param({ "10000", "100000", "200000", "400000" })
		int size;

		SwissSet<String> swiss;
		HashSet<String> jdk;
		ObjectOpenHashSet<String> fastutil;
		UnifiedSet<String> unified;
		String[] keys;
		String[] misses;
		int nextKeyIndex;
		int nextMissIndex;

		@Setup(Level.Trial)
		public void setup() {
			Random rnd = new Random(123);
			keys = new String[size];
			misses = new String[size];
			Set<String> keySet = new HashSet<>(size * 2);
			for (int i = 0; i < size; i++) {
				String k = nextUuidString(rnd);
				keys[i] = k;
				keySet.add(k);
			}
			for (int i = 0; i < size; i++) {
				String miss;
				do { miss = nextUuidString(rnd); } while (keySet.contains(miss));
				misses[i] = miss;
			}
			nextKeyIndex = 0;
			nextMissIndex = 0;

			swiss = new SwissSet<>();
			jdk = new HashSet<>();
			fastutil = new ObjectOpenHashSet<>();
			unified = new UnifiedSet<>();
			for (String k : keys) {
				swiss.add(k);
				jdk.add(k);
				fastutil.add(k);
				unified.add(k);
			}
		}

		String nextHitKey() {
			String k = keys[nextKeyIndex];
			nextKeyIndex = (nextKeyIndex + 1) % keys.length;
			return k;
		}

		String nextMissKey() {
			String k = misses[nextMissIndex];
			nextMissIndex = (nextMissIndex + 1) % misses.length;
			return k;
		}
	}

	@State(Scope.Thread)
	public static class MutateState {
		@Param({ "10000", "100000", "200000", "400000" })
		int size;

		String[] keys;
		String[] misses;
		int hitIndex;
		int missIndex;
		SwissSet<String> swiss;
		HashSet<String> jdk;
		ObjectOpenHashSet<String> fastutil;
		UnifiedSet<String> unified;
		Random rnd;

		@Setup(Level.Trial)
		public void initKeys() {
			rnd = new Random(456);
			keys = new String[size];
			misses = new String[size];
			Set<String> keySet = new HashSet<>(size * 2);
			for (int i = 0; i < size; i++) {
				String k = nextUuidString(rnd);
				keys[i] = k;
				keySet.add(k);
			}
			for (int i = 0; i < size; i++) {
				String miss;
				do { miss = nextUuidString(rnd); } while (keySet.contains(miss));
				misses[i] = miss;
			}
		}

		@Setup(Level.Iteration)
		public void resetSets() {
			swiss = new SwissSet<>();
			jdk = new HashSet<>();
			fastutil = new ObjectOpenHashSet<>();
			unified = new UnifiedSet<>();
			for (String k : keys) {
				swiss.add(k);
				jdk.add(k);
				fastutil.add(k);
				unified.add(k);
			}
			hitIndex = 0;
			missIndex = 0;
		}

		String nextHitKey() {
			String k = keys[hitIndex];
			hitIndex = (hitIndex + 1) % keys.length;
			return k;
		}

		String nextMissKey() {
			String k = misses[missIndex];
			missIndex = (missIndex + 1) % misses.length;
			return k;
		}
	}

	// contains hit/miss
//	@Benchmark
	public void swissContainsHit(ReadState s, Blackhole bh) { boolean res = s.swiss.contains(s.nextHitKey()); bh.consume(res); }

//	@Benchmark
	public void jdkContainsHit(ReadState s, Blackhole bh) { boolean res = s.jdk.contains(s.nextHitKey()); bh.consume(res); }

	@Benchmark
	public void fastutilContainsHit(ReadState s, Blackhole bh) { boolean res = s.fastutil.contains(s.nextHitKey()); bh.consume(res); }

	@Benchmark
	public void unifiedContainsHit(ReadState s, Blackhole bh) { boolean res = s.unified.contains(s.nextHitKey()); bh.consume(res); }

//	@Benchmark
	public void swissContainsMiss(ReadState s, Blackhole bh) { boolean res = s.swiss.contains(s.nextMissKey()); bh.consume(res); }

//	@Benchmark
	public void jdkContainsMiss(ReadState s, Blackhole bh) { boolean res = s.jdk.contains(s.nextMissKey()); bh.consume(res); }

	@Benchmark
	public void fastutilContainsMiss(ReadState s, Blackhole bh) { boolean res = s.fastutil.contains(s.nextMissKey()); bh.consume(res); }

	@Benchmark
	public void unifiedContainsMiss(ReadState s, Blackhole bh) { boolean res = s.unified.contains(s.nextMissKey()); bh.consume(res); }

	// add hit/miss
//	@Benchmark
	public void swissAddHit(MutateState s, Blackhole bh) { boolean res = s.swiss.add(s.nextHitKey()); bh.consume(res); }

//	@Benchmark
	public void jdkAddHit(MutateState s, Blackhole bh) { boolean res = s.jdk.add(s.nextHitKey()); bh.consume(res); }

	@Benchmark
	public void fastutilAddHit(MutateState s, Blackhole bh) { boolean res = s.fastutil.add(s.nextHitKey()); bh.consume(res); }

	@Benchmark
	public void unifiedAddHit(MutateState s, Blackhole bh) { boolean res = s.unified.add(s.nextHitKey()); bh.consume(res); }

//	@Benchmark
	public void swissAddMiss(MutateState s, Blackhole bh) { boolean res = s.swiss.add(s.nextMissKey()); bh.consume(res); }

//	@Benchmark
	public void jdkAddMiss(MutateState s, Blackhole bh) { boolean res = s.jdk.add(s.nextMissKey()); bh.consume(res); }

	@Benchmark
	public void fastutilAddMiss(MutateState s, Blackhole bh) { boolean res = s.fastutil.add(s.nextMissKey()); bh.consume(res); }

	@Benchmark
	public void unifiedAddMiss(MutateState s, Blackhole bh) { boolean res = s.unified.add(s.nextMissKey()); bh.consume(res); }

	private static String nextUuidString(Random rnd) {
		return new UUID(rnd.nextLong(), rnd.nextLong()).toString();
	}
}

