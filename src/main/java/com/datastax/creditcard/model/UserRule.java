package com.datastax.creditcard.model;

public class UserRule {
	
	private String userId;
	private String ruleId;
	private String ruleName;
	private String issuer;
	private double amount;
	private int noOfTransactions = -1;
	private int noOfDays = -1;
	public String getUserId() {
		return userId;
	}
	public void setUserId(String userId) {
		this.userId = userId;
	}
	public String getRuleId() {
		return ruleId;
	}
	public void setRuleId(String ruleId) {
		this.ruleId = ruleId;
	}
	public String getRuleName() {
		return ruleName;
	}
	public void setRuleName(String ruleName) {
		this.ruleName = ruleName;
	}
	public String getIssuer() {
		return issuer;
	}
	public void setIssuer(String issuer) {
		this.issuer = issuer;
	}
	public double getAmount() {
		return amount;
	}
	public void setAmount(double amount) {
		this.amount = amount;
	}
	public int getNoOfTransactions() {
		return noOfTransactions;
	}
	public void setNoOfTransactions(int noOfTransactions) {
		this.noOfTransactions = noOfTransactions;
	}
	public int getNoOfDays() {
		return noOfDays;
	}
	public void setNoOfDays(int noOfDays) {
		this.noOfDays = noOfDays;
	}
	@Override
	public String toString() {
		return "UserRule [userId=" + userId + ", ruleId=" + ruleId + ", ruleName=" + ruleName + ", issuer=" + issuer
				+ ", amount=" + amount + ", noOfTransactions=" + noOfTransactions + ", noOfDays=" + noOfDays + "]";
	} 
}
