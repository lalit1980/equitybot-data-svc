package com.equitybot.trade.data.converter;



import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.LinkedList;

import com.equitybot.trade.data.db.mongodb.tick.domain.Tick;
import com.equitybot.trade.data.util.TickConstant;

public class CustomTickBar {


    private LinkedList<Tick> tickList;
    private double highPrice;
    private double lowPrice;
    private double volume;
    private long listCount;
    private long instrumentToken;
    private long listMaxSize;

    public CustomTickBar(long instrumentToken, long listMaxSize) {
        this.tickList = new LinkedList<>();
        this.highPrice = -1.0;
        this.lowPrice = -1.0;
        this.volume = 0.0;
        this.listCount = 0;
        this.instrumentToken = instrumentToken;
        this.listMaxSize = listMaxSize;
    }

    public void addTick(Tick tick) {
        this.tickList.add(tick);
        this.highPrice = this.highPrice == -1.0 || this.highPrice < tick.getLastTradedPrice() ? tick.getLastTradedPrice() : this.highPrice;
        this.lowPrice = this.lowPrice == -1.0 || this.lowPrice > tick.getLastTradedPrice() ? tick.getLastTradedPrice() : this.lowPrice;
        this.volume = volume + tick.getLastTradedQuantity();
        listCount++;
    }


    public ZonedDateTime getEndTime() {
        if (tickList.size() == this.listMaxSize) {
            return ZonedDateTime.ofInstant(tickList.get(this.tickList.size() - 1).getTickTimestamp().toInstant(), ZoneId.systemDefault());
        }
        throw new RuntimeException(TickConstant.EXCEPTION_MESSAGE);

    }

    public double getOpenPrice() {
        if (tickList.size() == this.listMaxSize) {
            return tickList.get(0).getLastTradedPrice();
        }
        throw new RuntimeException(TickConstant.EXCEPTION_MESSAGE);
    }

    public double getClosePrice() {
        if (tickList.size() == this.listMaxSize) {
            return tickList.get(tickList.size() - 1).getLastTradedPrice();
        }
        throw new RuntimeException(TickConstant.EXCEPTION_MESSAGE);
    }

	public double getHighPrice() {
		return highPrice;
	}

	public double getLowPrice() {
		return lowPrice;
	}

	public double getVolume() {
		return volume;
	}

	public long getListCount() {
		return listCount;
	}

	public long getInstrumentToken() {
		return instrumentToken;
	}

	public long getListMaxSize() {
		return listMaxSize;
	}

}
