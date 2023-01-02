package org.pcchen;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.*;

/**
 * kafka工具类
 *
 * @author: ceek
 * @create: 2021-01-11 14:26
 */
public class KafkaUtils {
    /**
     * 获取kafka中所有topic信息
     * @param brokerListUrl
     * @return
     */
    public static Set<String> getAllTopics(String brokerListUrl) {
        Properties props = new Properties();
        // kafka servers
        props.put("bootstrap.servers", brokerListUrl);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        org.apache.kafka.clients.admin.AdminClient client = KafkaAdminClient.create(props);

        Set<String> topicSet = new HashSet<String>();
        try {
            ListTopicsResult listTopicsResult = client.listTopics();
            KafkaFuture<Collection<TopicListing>> listings = listTopicsResult.listings();

            Collection<TopicListing> topicListings = listings.get();
            Iterator<TopicListing> topicListingIterator = topicListings.iterator();
            while (topicListingIterator.hasNext()) {
                TopicListing topicListing = topicListingIterator.next();
                System.out.println("topicname:" + topicListing.name());
            }

            KafkaFuture<Set<String>> topicsKafkaFuture = listTopicsResult.names();
            topicSet = topicsKafkaFuture.get();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            return topicSet;
        }
    }

    /**
     * 获取kafka中所有消费者信息
     * @param brokerListUrl
     * @param topic
     * @return
     */
    public static Set<String> getAllGroupsForTopic(String brokerListUrl, String topic) {
        Properties props = new Properties();
        // kafka servers
        props.put("bootstrap.servers", brokerListUrl);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Set<String> groupIdSet = new HashSet<String>();

        org.apache.kafka.clients.admin.AdminClient client = KafkaAdminClient.create(props);
        try {
            ListConsumerGroupsResult listConsumerGroupsResult = client.listConsumerGroups();

            KafkaFuture<Collection<ConsumerGroupListing>> all = listConsumerGroupsResult.all();

            Collection<ConsumerGroupListing> consumerGroupListings = all.get();
//            Collection<ConsumerGroupListing> consumerGroupListings1 = all.get(3, TimeUnit.SECONDS);

            Iterator<ConsumerGroupListing> iterator1 = consumerGroupListings.iterator();
            while (iterator1.hasNext()) {
                ConsumerGroupListing next = iterator1.next();
                System.out.println(next.toString());
                groupIdSet.add(next.groupId());
            }
            /*
            //获取指定topic的partition等信息
            DescribeTopicsResult describeTopicsResult = client.describeTopics(Arrays.asList(topic));
            KafkaFuture<Map<String, TopicDescription>> all = describeTopicsResult.all();

            Map<String, TopicDescription> stringTopicDescriptionMap = all.get();
            Set<Map.Entry<String, TopicDescription>> entries = stringTopicDescriptionMap.entrySet();
            Iterator<Map.Entry<String, TopicDescription>> iterator = entries.iterator();
            for(; iterator.hasNext();) {
                Map.Entry<String, TopicDescription> next1 = iterator.next();
                String key = next1.getKey();
                TopicDescription value = next1.getValue();
                List<TopicPartitionInfo> partitions = value.partitions();
                System.out.println(key + "===" + partitions.toString());
            }*/
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            return groupIdSet;
        }
    }

    public static void main(String[] args) {
        KafkaUtils.getAllTopics("192.168.1.101:9092");
//        KafkaUtils.getAllGroupsForTopic("192.168.1.101:9092", "clicks");
    }
}
