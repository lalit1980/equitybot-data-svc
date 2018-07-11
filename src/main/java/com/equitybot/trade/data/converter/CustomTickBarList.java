package com.equitybot.trade.data.converter;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.kubernetes.TcpDiscoveryKubernetesIpFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.ta4j.core.Bar;
import org.ta4j.core.BaseBar;
import org.ta4j.core.BaseTimeSeries;
import org.ta4j.core.Decimal;
import org.ta4j.core.TimeSeries;

import com.equitybot.trade.data.db.mongodb.tick.domain.Tick;
import com.equitybot.trade.data.util.TickConstant;

@Service
@Scope(value = ConfigurableBeanFactory.SCOPE_SINGLETON)
public class CustomTickBarList {

	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	private Map<Long, CustomTickBar> workingTickBarMap;
	private Map<Long, TimeSeries> timeSeriesMap;

	@Value("${tick.barsize}")
	private long eachBarSize;

	@Value("${spring.kafka.producer.zerodha-timeseries-publish-topic}")
	private String timeSeriesProducerTopic;
	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	IgniteCache<String, TimeSeries> cache;
	private IgniteCache<Long, Double> cacheLastTradedPrice;

	public CustomTickBarList() {
		this.workingTickBarMap = new HashMap<>();
		this.timeSeriesMap = new HashMap<>();
		IgniteConfiguration cfg = new IgniteConfiguration();
		Ignite ignite = Ignition.start(cfg);
		Ignition.setClientMode(true);
		CacheConfiguration<String, TimeSeries> ccfg = new CacheConfiguration<String, TimeSeries>("TimeSeriesCache");
		CacheConfiguration<Long, Double> ccfgLastTradedPrice = new CacheConfiguration<Long, Double>("LastTradedPrice");
		this.cache = ignite.getOrCreateCache(ccfg);
		this.cacheLastTradedPrice = ignite.getOrCreateCache(ccfgLastTradedPrice);
	}

	public synchronized void addTick(Tick tick) {
		CustomTickBar customTickBar = workingTickBarMap.get(tick.getInstrumentToken());
		if (customTickBar == null) {
			customTickBar = new CustomTickBar(tick.getInstrumentToken(), this.eachBarSize);
			workingTickBarMap.put(tick.getInstrumentToken(), customTickBar);
		}
		customTickBar.addTick(tick);
		this.getWorkingMinuteTickListValidate(customTickBar);
	}

	public void getWorkingMinuteTickListValidate(CustomTickBar customTickBar) {
		if (customTickBar.getListCount() == this.eachBarSize) {
			CustomTickBar newCustomTickBar = new CustomTickBar(customTickBar.getInstrumentToken(), this.eachBarSize);
			this.addInSeries(customTickBar);
			this.workingTickBarMap.put(newCustomTickBar.getInstrumentToken(), newCustomTickBar);
		}
	}

	private void addInSeries(CustomTickBar customTickBar) {
		Duration barDuration = Duration.ofSeconds(60);

		Bar bar = new BaseBar(barDuration, customTickBar.getEndTime(), Decimal.valueOf(customTickBar.getOpenPrice()),
				Decimal.valueOf(customTickBar.getHighPrice()), Decimal.valueOf(customTickBar.getLowPrice()),
				Decimal.valueOf(customTickBar.getClosePrice()), Decimal.valueOf(customTickBar.getVolume()),
				Decimal.valueOf(customTickBar.getVolume()));
		bar.addTrade(customTickBar.getVolume(), customTickBar.getClosePrice());
		logger.info("Instrument Token: " + customTickBar.getInstrumentToken() + " Open Price: " + bar.getOpenPrice()
				+ " Close Price: " + bar.getClosePrice() + " High Price: " + bar.getMaxPrice() + " Low Price: "
				+ bar.getMinPrice() + " Volume: " + bar.getVolume() + " Trade Count: " + bar.getTrades() + " Amount: "
				+ bar.getAmount() + " Bar begin time: " + bar.getBeginTime() + " Bar TimePeriod duration: "
				+ bar.getTimePeriod());

		logger.info(bar.toString());
		TimeSeries timeSeries = this.timeSeriesMap.get(customTickBar.getInstrumentToken());
		if (timeSeries == null) {
			timeSeries = new BaseTimeSeries(String.valueOf(customTickBar.getInstrumentToken()));
			this.timeSeriesMap.put(customTickBar.getInstrumentToken(), timeSeries);
		}
		timeSeries.addBar(bar);
		cache.put(timeSeries.getName(), timeSeries);
		if (cacheLastTradedPrice!=null) {
			logger.info("*******Last Traded Price Cache: "+cacheLastTradedPrice.get(Long.parseLong(timeSeries.getName())));
		}else {
			logger.info("%%%%%%%%%%%%%%% gand fatttttttt gayi last traded price is empty %%%%%%%%%%%");
		}
		ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(timeSeriesProducerTopic,
				timeSeries.getName());
		future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
			@Override
			public void onSuccess(SendResult<String, String> result) {
				// logger.info("Sent message: " + result);
			}

			@Override
			public void onFailure(Throwable ex) {
				logger.info("Failed to send message");
			}
		});
		// kafkaTemplate.send(timeSeriesProducerTopic, timeSeries.getName());
		logger.info(TickConstant.LOG_ADD_IN_SERIES_INFO, customTickBar.getEndTime());
	}

	public TimeSeries getSeries(Long instrumentToken) {
		return this.timeSeriesMap.get(instrumentToken);
	}

	public IgniteCache<String, TimeSeries> getCache() {
		return cache;
	}

	public void backTest(Tick tick) {
		Duration barDuration = Duration.ofSeconds(60);
		ZoneId istZone = ZoneId.of("Asia/Kolkata");
		if (tick.isBackTestFlag()) {
			ZonedDateTime barEndTime = ZonedDateTime.ofInstant(tick.getTickTimestamp().toInstant(), istZone);
			
			Bar bar = new BaseBar(barDuration, barEndTime, Decimal.valueOf(tick.getOpenPrice()),
					Decimal.valueOf(tick.getHighPrice()), Decimal.valueOf(tick.getLowPrice()),
					Decimal.valueOf(tick.getClosePrice()), Decimal.valueOf(tick.getLastTradedQuantity()),
					Decimal.valueOf(tick.getLastTradedQuantity()));
			bar.addTrade(tick.getLastTradedQuantity(), tick.getLastTradedPrice());
			logger.info(bar.toString());
			TimeSeries timeSeries = this.timeSeriesMap.get(tick.getInstrumentToken());
			if (timeSeries == null) {
				logger.info("*************Series is null*******************");
				timeSeries = new BaseTimeSeries(String.valueOf(tick.getInstrumentToken()));
				this.timeSeriesMap.put(tick.getInstrumentToken(), timeSeries);
			}
			try {
			timeSeries.addBar(bar);
			}catch(IllegalArgumentException argumentException) {
				logger.error(" --  new -:"+bar);
				logger.error(" --  old -:"+timeSeries.getBarData().get(timeSeries.getBarData().size()-1));
				throw argumentException;
				
			}
			cache.put(timeSeries.getName(), timeSeries);
			ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(timeSeriesProducerTopic,
					timeSeries.getName());
			future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
				@Override
				public void onSuccess(SendResult<String, String> result) {
					logger.info("Sent message: " + result);
				}

				@Override
				public void onFailure(Throwable ex) {
					logger.info("Failed to send message");
				}
			});
		}

	}

	public IgniteCache<Long, Double> getCacheLastTradedPrice() {
		return cacheLastTradedPrice;
	}

	public void setCacheLastTradedPrice(IgniteCache<Long, Double> cacheLastTradedPrice) {
		this.cacheLastTradedPrice = cacheLastTradedPrice;
	}
}