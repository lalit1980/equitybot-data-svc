package com.equitybot.trade.data.messaging.kafka.listener;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.ta4j.core.AnalysisCriterion;
import org.ta4j.core.Bar;
import org.ta4j.core.BaseBar;
import org.ta4j.core.BaseStrategy;
import org.ta4j.core.BaseTimeSeries;
import org.ta4j.core.Decimal;
import org.ta4j.core.Rule;
import org.ta4j.core.TimeSeries;
import org.ta4j.core.TimeSeriesManager;
import org.ta4j.core.TradingRecord;
import org.ta4j.core.analysis.CashFlow;
import org.ta4j.core.analysis.criteria.AverageProfitableTradesCriterion;
import org.ta4j.core.analysis.criteria.RewardRiskRatioCriterion;
import org.ta4j.core.analysis.criteria.TotalProfitCriterion;
import org.ta4j.core.analysis.criteria.VersusBuyAndHoldCriterion;
import org.ta4j.core.indicators.SMAIndicator;
import org.ta4j.core.indicators.helpers.ClosePriceIndicator;
import org.ta4j.core.trading.rules.CrossedDownIndicatorRule;
import org.ta4j.core.trading.rules.CrossedUpIndicatorRule;
import org.ta4j.core.trading.rules.StopGainRule;
import org.ta4j.core.trading.rules.StopLossRule;

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
		customCalendar.add(Calendar.SECOND, 1);
		tick.setTickTimestamp(customCalendar.getTime());
		 customTickBarList.addTick(tick);
		// repository.saveTickData(tick);
		
	}
	
	
}
