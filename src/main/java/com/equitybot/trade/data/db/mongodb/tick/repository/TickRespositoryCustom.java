package com.equitybot.trade.data.db.mongodb.tick.repository;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.equitybot.trade.data.db.mongodb.tick.domain.Tick;
import com.mongodb.client.result.DeleteResult;

public interface TickRespositoryCustom {

	public List<Tick> findByTimeStamp(Date timestamp);
	public List<Tick> findByTokenAndTimeRange(long InstrumentToken, Date beginDate, Date endDate);
	public List<Tick> findByInstrumentToken(long token);
	public void saveTickData(Tick tick);
	public void saveAllTick(List<Tick> ticks);
	public DeleteResult deleteAllTicks();
	public List<Tick> findById(String id);
	public List<List<Tick>> findAllTicksByInstrumentToken(ArrayList<Long> instrumentTokens);
}
