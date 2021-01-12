package org.pcchen;


import kafka.admin.AdminClient;
import kafka.coordinator.group.GroupOverview;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import scala.Function1;
import scala.Tuple2;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * @author ceek
 * @create 2021-01-11 14:26
 **/
public class KafkaUtils {
    public static Set<String> getAllGroupsForTopic(String brokerListUrl, String topic) {

        Properties props = new Properties();
        // kafka servers
        props.put("bootstrap.servers", "10.10.32.60:9093");
//        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // group
//        props.put("enable.auto.commit", "false");
//        props.put("auto.commit.interval.ms", "1000");
//        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        org.apache.kafka.clients.admin.AdminClient client = KafkaAdminClient.create(props);
//        AdminClient client = AdminClient.createSimplePlaintext(brokerListUrl);

        ListTopicsResult listTopicsResult1 = client.listTopics();
        try {
            Collection<TopicListing> topicListings = listTopicsResult1.listings().get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        try {
            ListTopicsResult listTopicsResult = client.listTopics();
            KafkaFuture<Collection<TopicListing>> listings = listTopicsResult.listings();

            Collection<TopicListing> topicListings = listings.get();
            while (topicListings.iterator().hasNext()) {
                TopicListing next = (TopicListing) topicListings.iterator().next();

            }

            KafkaFuture<Set<String>> names = listTopicsResult.names();
            System.out.println(names.get());

            ListConsumerGroupsResult listConsumerGroupsResult = client.listConsumerGroups();
            KafkaFuture<Collection<ConsumerGroupListing>> all = listConsumerGroupsResult.all();

            Collection<ConsumerGroupListing> consumerGroupListings = all.get();
            Collection<ConsumerGroupListing> consumerGroupListings1 = all.get(3, TimeUnit.SECONDS);


            try {
                while (all.get().iterator().hasNext()) {
                    ConsumerGroupListing next = all.get().iterator().next();
                    System.out.println(next.toString());
                    System.out.println(next.toString());
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) {
        KafkaUtils.getAllGroupsForTopic("192.168.75.128:9092", "test_find");
    }
}
