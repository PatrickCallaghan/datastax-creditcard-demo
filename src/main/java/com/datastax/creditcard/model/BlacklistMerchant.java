package com.datastax.creditcard.model;

import java.util.Map;

public class BlacklistMerchant {
	
	private Map<String, Double> cityAmount;
	
	public BlacklistMerchant(){		
	}

	public Map<String, Double> getCityAmount() {
		return cityAmount;
	}
	public void setCityAmount(Map<String, Double> cityAmount) {
		this.cityAmount = cityAmount;
	}
}
