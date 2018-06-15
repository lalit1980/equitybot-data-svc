package com.equitybot.trade.data.db.mongodb.tick.domain;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import com.google.gson.annotations.SerializedName;

@Document(collection = "Depth")
public class Depth {
	
	@Id
	//@Indexed(unique = true)
	private String id;
	@SerializedName("quantity")
    private int quantity;
    @SerializedName("price")
    private double price;
    @SerializedName("orders")
    private int orders;
	public int getQuantity() {
		return quantity;
	}
	public void setQuantity(int quantity) {
		this.quantity = quantity;
	}
	public double getPrice() {
		return price;
	}
	public void setPrice(double price) {
		this.price = price;
	}
	public int getOrders() {
		return orders;
	}
	public void setOrders(int orders) {
		this.orders = orders;
	}
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	@Override
	public String toString() {
		return "Depth [id=" + id + ", quantity=" + quantity + ", price=" + price + ", orders=" + orders + "]";
	}

}
