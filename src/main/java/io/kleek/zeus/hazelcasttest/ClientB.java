/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.kleek.zeus.hazelcasttest;

import com.hazelcast.config.Config;
import com.hazelcast.core.DistributedObjectEvent;
import com.hazelcast.core.DistributedObjectListener;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.ReplicatedMap;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author zulk
 */
public class ClientB implements DistributedObjectListener {

    public static void main(String[] args) throws IOException {
        Config config = new Config("master");

        config.getGroupConfig().setName("master1").setPassword("ala");

        HazelcastInstance hz = Hazelcast
                .newHazelcastInstance(config);

        ClientB clientB = new ClientB();

        hz.addDistributedObjectListener(clientB);
//        TransactionContext trc = hz.newTransactionContext();

        ReplicatedMap<Integer, String> map = hz.getReplicatedMap("test");
        ITopic<Object> topic = hz.getTopic("messages");

        ScheduledExecutorService exec = Executors
                .newSingleThreadScheduledExecutor();

        Random random = new Random();

        Runnable cons = () -> {
            int rand = random.nextInt(10) + 1;
            Optional<String> mayFound = findKey(rand, map, topic);
            if (mayFound.isPresent()) {
                System.out.println("KEY: " + mayFound.get());
            } else {
                System.out.println("NOT FOUND: " + rand);
            }
        };

        exec.scheduleAtFixedRate(cons, 3, 10, TimeUnit.SECONDS);

        System.in.read();
        hz.shutdown();
        exec.shutdownNow();
    }

    private static Optional<String> findKey(Integer i, Map<Integer, String> map, ITopic<Object> topic) {
        final String value = map.get(i);
        if (value == null) {
            topic.publish(i);
            return Optional.empty();
        } else {
            return Optional.of(value);
        }
    }

    @Override
    public void distributedObjectCreated(DistributedObjectEvent doe) {
        System.out.println("B -> " + doe);
    }

    @Override
    public void distributedObjectDestroyed(DistributedObjectEvent doe) {
        System.out.println("B <- " + doe);
    }

}
