package io.druid.hyper.client.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class PartitionUtil {

    private static final HashFunction hashFunction = Hashing.murmur3_32();
    private static final ObjectMapper jsonMapper = new ObjectMapper();

    public static int getPartitionNum(Object value, int partitions) {
        try {
            return Math.abs(hashFunction.hashBytes(jsonMapper.writeValueAsBytes(value)).asInt()) % partitions;
        }
        catch (JsonProcessingException e) {
            throw Throwables.propagate(e);
        }
    }
}
