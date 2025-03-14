package com.distsys.a2.callback;

import java.util.Map;

public interface ReduceCallback<E, K, V> {
    void reduceDone(E e, Map<K, V> results);
}
