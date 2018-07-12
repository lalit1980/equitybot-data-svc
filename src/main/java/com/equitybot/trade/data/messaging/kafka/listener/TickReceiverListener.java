package com.equitybot.trade.data.messaging.kafka.listener;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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
import com.zerodhatech.models.Depth;

public class TickReceiverListener {

	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	@Value("${spring.kafka.bootstrap-servers}")
	private String bootstrapServers;

	@Value("${spring.kafka.producer.topic-data-seriesupdate}")
	private String timeSeriesProducerTopic;

	@Autowired
	private CustomTickBarList customTickBarList;
	
	@Autowired
	TickRepository repository;

	@Autowired
	TickSerializer serializer;
	
	@Value("${tick.backTestFlag}")
	private boolean backTestFlag;

	@KafkaListener(id = "id0", topicPartitions = {
			@TopicPartition(topic = "topic-kite-tick", partitions = { "0" }) })
	public void listenPartition0(ConsumerRecord<?, ?> record) throws IOException {
		Gson gson = new Gson();
		com.zerodhatech.models.Tick tickList = gson.fromJson(record.value().toString(), com.zerodhatech.models.Tick.class);
		processRequest(tickList);
	}

	@KafkaListener(id = "id1", topicPartitions = {
			@TopicPartition(topic = "topic-kite-tick", partitions = { "1" }) })
	public void listenPartition1(ConsumerRecord<?, ?> record) throws IOException {
		Gson gson = new Gson();
		com.zerodhatech.models.Tick tickList = gson.fromJson(record.value().toString(), com.zerodhatech.models.Tick.class);
		processRequest(tickList);
	}

	@KafkaListener(id = "id2", topicPartitions = {
			@TopicPartition(topic = "topic-kite-tick", partitions = { "2" }) })
	public void listenPartition2(ConsumerRecord<?, ?> record) throws IOException {
		Gson gson = new Gson();
		com.zerodhatech.models.Tick tickList = gson.fromJson(record.value().toString(), com.zerodhatech.models.Tick.class);
		processRequest(tickList);
	}

	private void processRequest(com.zerodhatech.models.Tick tick) throws IOException {
		if(backTestFlag) {
			Tick tickz=convertTickModel(tick);
			logger.info("Received: "+tickz.toString());
			customTickBarList.backTest(tickz);
			repository.saveTickData(tickz);
		}else {
			Tick tickz=convertTickModel(tick);
			logger.info(tickz.toString());
			 customTickBarList.addTick(tickz);
			 repository.saveTickData(tickz);
		}
	}
	
	public com.equitybot.trade.data.db.mongodb.tick.domain.Tick convertTickModel(com.zerodhatech.models.Tick tick) {
		com.equitybot.trade.data.db.mongodb.tick.domain.Tick tickModel=null;
		if(tick!=null) {
			tickModel = new com.equitybot.trade.data.db.mongodb.tick.domain.Tick();
			tickModel.setAverageTradePrice(tick.getAverageTradePrice());
			tickModel.setChange(tick.getChange());
			tickModel.setClosePrice(tick.getClosePrice());
			tickModel.setHighPrice(tick.getHighPrice());
			tickModel.setId(UUID.randomUUID().toString());
			tickModel.setInstrumentToken(tick.getInstrumentToken());
			tickModel.setLastTradedPrice(tick.getLastTradedPrice());
			tickModel.setLastTradedQuantity(tick.getLastTradedQuantity());
			tickModel.setLastTradedTime(tick.getLastTradedTime());
			tickModel.setLowPrice(tick.getLowPrice());
			tickModel.setMode(tick.getMode());
			tickModel.setOi(tick.getOi());
			tickModel.setOiDayHigh(tick.getOpenInterestDayHigh());
			tickModel.setOiDayLow(tick.getOpenInterestDayLow());
			tickModel.setOpenPrice(tick.getOpenPrice());
			tickModel.setTickTimestamp(tick.getTickTimestamp());
			tickModel.setTotalBuyQuantity(tick.getTotalBuyQuantity());
			tickModel.setTotalSellQuantity(tick.getTotalSellQuantity());
			tickModel.setTradable(tick.isTradable());
			tickModel.setVolumeTradedToday(tick.getVolumeTradedToday());
			Map<String, ArrayList<com.equitybot.trade.data.db.mongodb.tick.domain.Depth>> depth=new HashMap<String, ArrayList<com.equitybot.trade.data.db.mongodb.tick.domain.Depth>>();
			if(tick.getMarketDepth()!=null && tick.getMarketDepth().size()>0) {
				Map<String, ArrayList<Depth>> marketDepth=tick.getMarketDepth();
				marketDepth.forEach((k,v)->{
					 List<Depth> depthList=v;
					 ArrayList<com.equitybot.trade.data.db.mongodb.tick.domain.Depth> mongoDepthList=new ArrayList<com.equitybot.trade.data.db.mongodb.tick.domain.Depth>();
					 depthList.forEach(item->{
						 com.equitybot.trade.data.db.mongodb.tick.domain.Depth depthObj=new com.equitybot.trade.data.db.mongodb.tick.domain.Depth();
						 depthObj.setId(UUID.randomUUID().toString());	
						 depthObj.setOrders(item.getOrders());
							depthObj.setPrice(item.getPrice());
							depthObj.setQuantity(item.getQuantity());
							mongoDepthList.add(depthObj);
						});
					 depth.put(k, mongoDepthList);
				 });
				 tickModel.setDepth(depth);
			}
			
		}
	
		return tickModel;
	}
}
