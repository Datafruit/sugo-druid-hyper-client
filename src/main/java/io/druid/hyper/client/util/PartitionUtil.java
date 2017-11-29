package io.druid.hyper.client.util;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class PartitionUtil {

    private static final HashFunction hashFunction = Hashing.murmur3_32();

    public static int getPartitionNum(Object value, int partitions) {
        return Math.abs(hashFunction.hashBytes(value.toString().getBytes()).asInt()) % partitions;
    }
}
