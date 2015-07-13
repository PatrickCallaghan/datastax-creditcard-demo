package com.datastax.creditcard.model;

public class Merchant {

	private String id;
	private String name;
	private String location;
	
	public Merchant(String id, String name, String location) {
		super();
		this.id = id;
		this.name = name;
		this.location = location;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getLocation() {
		return location;
	}
	public void setLocation(String location) {
		this.location = location;
	}
	@Override
	public String toString() {
		return "Issuer {id=" + id + ", name=" + name + ", location=" + location + "}";
	}
}
