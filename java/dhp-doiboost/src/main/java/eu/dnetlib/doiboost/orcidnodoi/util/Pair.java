
package eu.dnetlib.doiboost.orcidnodoi.util;

public class Pair<K, V> {

	private final K k;

	private final V v;

	public Pair(K k, V v) {
		this.k = k;
		this.v = v;
	}

	public K getKey() {
		return k;
	}

	public V getValue() {
		return v;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Pair<?, ?>) {
			Pair<?, ?> tmp = (Pair<?, ?>) obj;
			return k.equals(tmp.getKey()) && v.equals(tmp.getValue());
		} else
			return false;
	}

}
