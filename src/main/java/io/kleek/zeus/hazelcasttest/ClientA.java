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
import java.io.IOException;
import java.time.LocalDate;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

/**
 *
 * @author zulk
 */
public class ClientA implements DistributedObjectListener {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException {
        Config config = new Config("master");

        config.getGroupConfig().setName("master1").setPassword("ala");

        HazelcastInstance hz = Hazelcast
                .newHazelcastInstance(config);

        ClientA clientA = new ClientA();

        hz.addDistributedObjectListener(clientA);

        Map<Integer, String> map = hz.getReplicatedMap("test");

        hz.getTopic("messages").addMessageListener(m -> {
            System.out.println("MESSAGE " + m.getMessageObject());
            map.putIfAbsent((Integer) m.getMessageObject(), "from message " + m
                    .getMessageObject());
        });

        ScheduledExecutorService exec = Executors
                .newSingleThreadScheduledExecutor();

        executeInBack(exec, map);

        System.in.read();
        hz.shutdown();
        exec.shutdownNow();
    }

    private static void executeInBack(ScheduledExecutorService exec, Map<Integer, String> map) {
        Runnable r = () -> {
            System.out.println("CLEAR");
            IntStream.range(1, 11).forEach(i -> map
                    .put(i, "new " + LocalDate.now().toEpochDay()));
            System.out.println("CLEAR END");

        };
        exec.scheduleAtFixedRate(r, 10, 30, TimeUnit.SECONDS);
    }

    @Override
    public void distributedObjectCreated(DistributedObjectEvent doe) {
        System.out.println("A -> " + doe);
    }

    @Override
    public void distributedObjectDestroyed(DistributedObjectEvent doe) {
        System.out.println("A <- " + doe);
    }


}
