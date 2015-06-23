package com.datastax.creditcard.rules;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.datastax.creditcard.model.BlacklistIssuer;
import com.datastax.creditcard.model.Transaction;
import com.datastax.creditcard.model.Transaction.Status;

public abstract class AbstractRule implements Rule{

	
	Map<String, BlacklistIssuer> issuerBlackList = new HashMap<String, BlacklistIssuer>();
	Map<String, Double> ccNoBlackMap = new HashMap<String, Double>();
	List<Transaction> transactions = new ArrayList<Transaction>();

	@Override
	public void setIssuerBlackList(Map<String, BlacklistIssuer> issuerBlackList) {
		this.issuerBlackList = issuerBlackList;
	}
	
	@Override
	public void setCCNoBlackList(Map<String, Double> ccNoBlackMap) {
		this.ccNoBlackMap = ccNoBlackMap;		
	}

	@Override
	public void setLatestTransactions(List<Transaction> transactions) {
		this.transactions = transactions;		
	}
	
	public Transaction getLastTransaction(){
		if (!this.transactions.isEmpty()){		
			return transactions.get(0);
		}else {
			return null;
		}
	}
	
	@Override
	abstract public Status processRule(Transaction transaction); 
}
