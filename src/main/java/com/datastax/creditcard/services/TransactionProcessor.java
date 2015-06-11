package com.datastax.creditcard.services;

import com.datastax.creditcard.dao.CreditCardDao;
import com.datastax.demo.utils.PropertyHelper;

public class TransactionProcessor {
	
	public TransactionProcessor(){
		String contactPointsStr = PropertyHelper.getProperty("contactPoints", "127.0.0.1");
		String noOfCreditCardsStr = PropertyHelper.getProperty("noOfCreditCards", "10000000");
		String noOfTransactionsStr = PropertyHelper.getProperty("noOfTransactions", "50000");

		CreditCardDao dao = new CreditCardDao(contactPointsStr.split(","));

		int noOfTransactions = Integer.parseInt(noOfTransactionsStr);
		int noOfCreditCards = Integer.parseInt(noOfCreditCardsStr);

		//Initialize credit cards;		
		dao.getCreditCardNosIter();
		//dao.getCreditCardNosAll();		
		
		System.exit(0);
	}

	public static void main(String[] args) {
		new TransactionProcessor();
	}

}
