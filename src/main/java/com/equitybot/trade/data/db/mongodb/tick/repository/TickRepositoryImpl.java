package com.equitybot.trade.data.db.mongodb.tick.repository;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.BulkOperations.BulkMode;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import com.equitybot.trade.data.db.mongodb.tick.domain.Tick;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.result.DeleteResult;

public class TickRepositoryImpl implements TickRespositoryCustom {

	@Autowired
	MongoTemplate mongoTemplate;

	@Override
	public List<Tick> findByTimeStamp(Date timestamp) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void saveTickData(Tick tick) {
		mongoTemplate.save(tick);

	}

	@Override
	public void saveAllTick(List<Tick> ticks) {
		BulkOperations bulkOperations = mongoTemplate.bulkOps(BulkMode.UNORDERED, Tick.class);
		ticks.forEach(tick -> {
			bulkOperations.insert(tick);
		});
		BulkWriteResult result = bulkOperations.execute();
	}

	@Override
	public List<Tick> findByInstrumentToken(long token) {
		Query query = new Query();
		query.with(new Sort(Sort.Direction.ASC, "tickTimestamp"));
		query.addCriteria(Criteria.where("instrumentToken").is(token));
		return mongoTemplate.find(query, Tick.class);
	}

	@Override
	public DeleteResult deleteAllTicks() {
		return mongoTemplate.remove(new Query(), "Tick");

	}

	@Override
	public List<Tick> findByTokenAndTimeRange(long InstrumentToken, Date beginDate, Date endDate) {
		Query query = new Query();

		query.with(new Sort(Sort.Direction.ASC, "tickTimestamp"));
		query.addCriteria(Criteria.where("instrumentToken").is(InstrumentToken));
		List<Tick> list = mongoTemplate.find(query, Tick.class);
		return list;
	}

	@Override
	public List<Tick> findById(String id) {
		
		
		return mongoTemplate.find(new Query(Criteria.where("id").is(id)), Tick.class);
	}

	@Override
	public List<List<Tick>> findAllTicksByInstrumentToken(ArrayList<Long> instrumentTokens) {
		List<Tick> tickList=new ArrayList<Tick>();
		List<List<Tick>> masterList=new ArrayList<List<Tick>>();
		for (Long token : instrumentTokens) {
			tickList=findByInstrumentToken(token);	
			masterList.add(tickList);
		}
		
		return masterList;
		
	}
}
