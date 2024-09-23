package io.github.vcvitaly;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.kafka.clients.producer.internals.BuiltInPartitioner;
import org.apache.kafka.common.serialization.StringSerializer;

public class Main {
    public static void main(String[] args) {
        final Stream<String> poles = IntStream.rangeClosed(1, 1000)
                .mapToObj(i -> "ELECTRIC_POLE_" + i);
        final Stream<String> cables = IntStream.rangeClosed(1, 2000)
                .mapToObj(i -> "CABLE_" + i);
        final Stream<String> assetGuids = Stream.concat(poles, cables);
        Map<Integer, Long> distribution;
        try (final StringSerializer stringSerializer = new StringSerializer()) {
            distribution = assetGuids
                    .collect(Collectors.groupingBy(guid -> getPartition(stringSerializer, guid, 8), Collectors.counting()));
        }

        System.out.println(distribution);
    }

    private static int getPartition(StringSerializer stringSerializer, String key, int numPartitions) {
        final byte[] serializedKey = stringSerializer.serialize(null, key);
        return BuiltInPartitioner.partitionForKey(serializedKey, numPartitions);
    }
}