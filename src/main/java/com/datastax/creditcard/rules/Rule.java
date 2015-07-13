package com.datastax.creditcard.rules;

import java.util.List;
import java.util.Map;

import com.datastax.creditcard.model.BlacklistMerchant;
import com.datastax.creditcard.model.Transaction;
import com.datastax.creditcard.model.Transaction.Status;

public interface Rule {

	public void setIssuerBlackList(Map<String, BlacklistMerchant> issuerBlackList);
	
	public void setCCNoBlackList(Map<String, Double> issuerBlackList);

	public void setLatestTransactions(List<Transaction> transactions);
	
	public Status processRule(Transaction transaction);
}
