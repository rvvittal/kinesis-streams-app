package com.amazonaws.services.kinesis.samples.streams.model;

import java.io.IOException;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public class StockOrder  {

	private final static ObjectMapper JSON = new ObjectMapper();
	static {
		JSON.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	}

	/**
	 * Represents the type of the stock trade eg buy or sell.
	 */
	public enum TradeType {
		BUY, SELL
	}

	String tickerSymbol;
	double amount;
	long orderId;
	TradeType tradeType;
	StockTrade stockTrade;

	public StockOrder(String tickerSymbol, double amount, long orderId, TradeType tradeType) {
		
		this.tickerSymbol = tickerSymbol;
		this.amount = amount;
		this.orderId = orderId;
		this.tradeType = tradeType;
	}

	public byte[] toJsonAsBytes() {
		try {
			return JSON.writeValueAsBytes(this);
		} catch (IOException e) {
			return null;
		}
	}
	
	public static StockOrder fromJsonAsBytes(byte[] bytes) {
        try {
            return JSON.readValue(bytes, StockOrder.class);
        } catch (IOException e) {
            return null;
        }
    }

	public String getTickerSymbol() {
		return tickerSymbol;
	}

	public void setTickerSymbol(String tickerSymbol) {
		this.tickerSymbol = tickerSymbol;
	}

	public double getAmount() {
		return amount;
	}

	public void setAmount(double amount) {
		this.amount = amount;
	}

	public long getOrderId() {
		return orderId;
	}

	public void setOrderId(long orderId) {
		this.orderId = orderId;
	}

	public TradeType getTradeType() {
		return tradeType;
	}

	public void setTradeType(TradeType tradeType) {
		this.tradeType = tradeType;
	}

	public StockTrade getStockTrade() {
		return stockTrade;
	}

	public void setStockTrade(StockTrade stockTrade) {
		this.stockTrade = stockTrade;
	}

	@Override
	public String toString() {
		return String.format("OrderId: %d ticker: %s type: %s amount: $%.02f", orderId, tickerSymbol, tradeType,
				amount);
	}

}
