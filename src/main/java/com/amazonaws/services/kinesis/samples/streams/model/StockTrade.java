package com.amazonaws.services.kinesis.samples.streams.model;

import java.io.IOException;


import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public class StockTrade  {

	private final static ObjectMapper JSON = new ObjectMapper();
	static {
		JSON.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	}

	double price;
	double amount;
	long orderId;
	long tradeId;
	long quantity;
	String tickerSymbol;


	public String getTickerSymbol() {
		return tickerSymbol;
	}

	public void setTickerSymbol(String tickerSymbol) {
		this.tickerSymbol = tickerSymbol;
	}

	public StockTrade(String tickerSymbol, long orderId, long tradeId, double amount, double price, long quantity) {
		
		this.tickerSymbol = tickerSymbol;
		this.amount = amount;
		this.tradeId = tradeId;
		this.orderId = orderId;
		this.price = price;
		this.quantity = quantity;

	}

	public byte[] toJsonAsBytes() {
		try {
			return JSON.writeValueAsBytes(this);
		} catch (IOException e) {
			return null;
		}
	}

	public static StockTrade fromJsonAsBytes(byte[] bytes) {
		try {
			return JSON.readValue(bytes, StockTrade.class);
		} catch (IOException e) {
			return null;
		}
	}

	@Override
	public String toString() {
		return String.format("orderId: %d tradeId: %d %d shares of %s for price $%.02f with total $%.02f", orderId,
				tradeId, quantity, tickerSymbol, price, amount);
	}

	public double getPrice() {
		return price;
	}

	public void setPrice(double price) {
		this.price = price;
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

	public long getTradeId() {
		return tradeId;
	}

	public void setTradeId(long tradeId) {
		this.tradeId = tradeId;
	}

	public long getQuantity() {
		return quantity;
	}

	public void setQuantity(long quantity) {
		this.quantity = quantity;
	}

}
