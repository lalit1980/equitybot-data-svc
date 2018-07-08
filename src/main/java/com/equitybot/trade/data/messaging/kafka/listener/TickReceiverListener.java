package com.equitybot.trade.data.messaging.kafka.listener;

import java.io.IOException;
import java.util.Calendar;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;

import com.equitybot.trade.data.converter.CustomTickBarList;
import com.equitybot.trade.data.db.mongodb.tick.domain.Tick;
import com.equitybot.trade.data.db.mongodb.tick.repository.TickRepository;
import com.equitybot.trade.data.util.TickSerializer;
import com.google.gson.Gson;

public class TickReceiverListener {

	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	@Value("${spring.kafka.bootstrap-servers}")
	private String bootstrapServers;

	@Value("${spring.kafka.producer.zerodha-timeseries-publish-topic}")
	private String timeSeriesProducerTopic;

	@Autowired
	private CustomTickBarList customTickBarList;
	
	@Autowired
	TickRepository repository;

	@Autowired
	TickSerializer serializer;

	private static Calendar customCalendar = Calendar.getInstance();

	@KafkaListener(id = "id0", topicPartitions = {
			@TopicPartition(topic = "zerodha-tickdataservice-publish", partitions = { "0" }) })
	public void listenPartition0(ConsumerRecord<?, ?> record) throws IOException {
		Gson gson = new Gson();
		Tick tickList = gson.fromJson(record.value().toString(), Tick.class);
		processRequest(tickList);
	}

	@KafkaListener(id = "id1", topicPartitions = {
			@TopicPartition(topic = "zerodha-tickdataservice-publish", partitions = { "1" }) })
	public void listenPartition1(ConsumerRecord<?, ?> record) throws IOException {
		Gson gson = new Gson();
		Tick tickList = gson.fromJson(record.value().toString(), Tick.class);
		processRequest(tickList);
	}

	@KafkaListener(id = "id2", topicPartitions = {
			@TopicPartition(topic = "zerodha-tickdataservice-publish", partitions = { "2" }) })
	public void listenPartition2(ConsumerRecord<?, ?> record) throws IOException {
		Gson gson = new Gson();
		Tick tickList = gson.fromJson(record.value().toString(), Tick.class);
		processRequest(tickList);
	}

	private void processRequest(Tick tick) throws IOException {
		if(tick.isBackTestFlag()) {
			customTickBarList.backTest(tick);
			//repository.saveTickData(tick);
		}else {
			 customTickBarList.addTick(tick);
			 repository.saveTickData(tick);
		}
	}
}
