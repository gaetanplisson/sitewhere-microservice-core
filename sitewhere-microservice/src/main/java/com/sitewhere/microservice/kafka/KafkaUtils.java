/**
 * Copyright © 2014-2021 The SiteWhere Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.sitewhere.microservice.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

import com.sitewhere.microservice.configuration.model.instance.infrastructure.KafkaConfiguration;
import com.sitewhere.spi.SiteWhereException;
import com.sitewhere.spi.microservice.IMicroservice;

public class KafkaUtils {

    /** Wait of the description of the topics */
    private static final long PARTITIONS_TIMEOUT_SECONDS = 60;

    /**
     * Get bootstrap servers from microservice configuration.
     * 
     * @param microservice
     * @return
     */
    public static String getBootstrapServers(IMicroservice<?, ?> microservice) {
	String systemNamespace = microservice.getInstanceConfiguration().getInfrastructure().getNamespace();
	KafkaConfiguration kafka = microservice.getInstanceConfiguration().getInfrastructure().getKafka();
	return String.format("%s.%s:%d", kafka.getHostname(), systemNamespace, kafka.getPort());
    }

    /**
     * Partitions of the topics, described by an admin client rather than by
     * consumer.partitionsFor(): on a new connection, the consumer sends its own
     * metadata update next to the one of partitionsFor(), two requests in flight
     * on the connection, which the echarde filter does not correlate (the answer
     * of partitionsFor() is lost, its timeout closes the connection, and the next
     * attempt does the same). The admin client sends its calls once its metadata
     * are ready, one request at a time.
     * 
     * @param microservice
     * @param topics
     * @return
     * @throws Exception
     */
    public static List<TopicPartition> getPartitions(IMicroservice<?, ?> microservice, List<String> topics)
	    throws Exception {
	Properties config = new Properties();
	config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers(microservice));
	try (AdminClient admin = AdminClient.create(config)) {
	    List<TopicPartition> partitions = new ArrayList<>();
	    for (TopicDescription topic : admin.describeTopics(topics).all()
		    .get(PARTITIONS_TIMEOUT_SECONDS, TimeUnit.SECONDS).values()) {
		if (topic.partitions().isEmpty()) {
		    throw new SiteWhereException("No partitions found for topic " + topic.name());
		}
		for (TopicPartitionInfo info : topic.partitions()) {
		    partitions.add(new TopicPartition(topic.name(), info.partition()));
		}
	    }
	    return partitions;
	}
    }
}
