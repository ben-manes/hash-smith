package com.donghyungko.swisstable;

/**
 * Skeleton for a Robin Hood hashing map implementation.
 * Null keys are NOT allowed; null values are allowed.
 */
public class RobinHoodMap<K, V> {

	/* Defaults */
	private static final int DEFAULT_INITIAL_CAPACITY = 16;
	private static final double DEFAULT_LOAD_FACTOR = 0.75d;

	/* Storage */
	private Object[] keys;
	private Object[] vals;
	private byte[] dist; // probe distance
	private int size;
	private int capacity;
	private int maxLoad;
	private double loadFactor = DEFAULT_LOAD_FACTOR;

	public RobinHoodMap() {
		this(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR);
	}

	public RobinHoodMap(int initialCapacity) {
		this(initialCapacity, DEFAULT_LOAD_FACTOR);
	}

	public RobinHoodMap(int initialCapacity, double loadFactor) {
		validateLoadFactor(loadFactor);
		this.loadFactor = loadFactor;
		resize(initialCapacity);
	}

	private int calcMaxLoad(int cap) {
		int ml = (int) (cap * loadFactor);
		return Math.max(1, Math.min(ml, cap - 1));
	}

	/* Resize/rebuild helpers */
	private void resize(int newCapacity) {
		int cap = ceilPow2(Math.max(DEFAULT_INITIAL_CAPACITY, newCapacity));
		Object[] oldKeys = this.keys;
		Object[] oldVals = this.vals;

		this.capacity = cap;
		this.keys = new Object[cap];
		this.vals = new Object[cap];
		this.dist = new byte[cap];
		this.size = 0;
		this.maxLoad = calcMaxLoad(cap);

		if (oldKeys == null || oldVals == null || oldKeys.length == 0) return;

		for (int i = 0; i < oldKeys.length; i++) {
			Object k = oldKeys[i];
			if (k == null) continue;
			@SuppressWarnings("unchecked")
			K key = (K) k;
			@SuppressWarnings("unchecked")
			V val = (V) oldVals[i];
			insertFresh(key, val, hash(key));
		}
	}

	/**
	 * Insert into a fresh table (dist/keys/vals are empty).
	 * Uses standard linear probing with distance tracking; no resize here.
	 */
	private void insertFresh(K key, V value, int h) {
		int mask = capacity - 1;
		int idx = h & mask; // equivalent to h % capacity
		int d = 0;
		while (keys[idx] != null) {
			idx = (idx + 1) & mask;
			d++;
		}
		keys[idx] = key;
		vals[idx] = value;
		dist[idx] = (byte) d;
		size++;
	}

	/* Hash helpers */
	private int hash(Object key) {
		if (key == null) throw new NullPointerException("Null keys not supported");
		int h = key.hashCode();
		return smear(h);
	}

	private int smear(int h) {
		h ^= (h >>> 16);
		return h;
	}

	/* Capacity helpers */
	private int ceilPow2(int x) {
		if (x <= 1) return 1;
		return Integer.highestOneBit(x - 1) << 1;
	}

	private void validateLoadFactor(double lf) {
		if (!(lf > 0.0d && lf < 1.0d)) {
			throw new IllegalArgumentException("loadFactor must be in (0,1): " + lf);
		}
	}
}

