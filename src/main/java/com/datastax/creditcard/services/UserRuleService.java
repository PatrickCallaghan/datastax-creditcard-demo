package com.datastax.creditcard.services;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.creditcard.model.Transaction;
import com.datastax.creditcard.model.Transaction.Status;
import com.datastax.creditcard.model.UserRule;

public class UserRuleService {
	
	private static Logger logger = LoggerFactory.getLogger(UserRuleService.class);
	
	public Transaction.Status processUserRule(Transaction transaction, UserRule userRule, List<Transaction> latestTransactions){
			
		if (!userRule.getMerchant().equals(transaction.getMerchant())){
			return Status.APPROVED;
		}
		
		if (userRule.getAmount() > 0){

			String issuer = userRule.getMerchant();
			double totalAmountForIssuer = this.findAmountForAllTransactionsForIssuer(issuer, latestTransactions);
			totalAmountForIssuer += transaction.getAmount();
			
			logger.info("Total amount will be  " + issuer + " : " + totalAmountForIssuer);
			
			if (totalAmountForIssuer > userRule.getAmount()){
				transaction.setNotes("Total Amount will be " + totalAmountForIssuer + " for issuer " + issuer + "." + userRule.getRuleName() + " needs approval for any more transactions.");
				transaction.setStatus(Status.CLIENT_APPROVAL.toString());
				return Status.CLIENT_APPROVAL;
			}

		}else if (userRule.getNoOfTransactions() > 0){
			
			String merchant = userRule.getMerchant();
			int noOfTransactions = findNoTransactionsForIssuer(merchant, latestTransactions);
	
			if (noOfTransactions > userRule.getNoOfTransactions()){
				transaction.setNotes("No of transaction is " + noOfTransactions + " for merchant " + merchant + "." + userRule.getRuleName() + " needs approval for more than " + userRule.getNoOfTransactions() + " transactions");
				transaction.setStatus(Status.CLIENT_APPROVAL.toString());
				return Status.CLIENT_APPROVAL;
			}
			
		}else{
			
			
		}
		
		return Status.valueOf(transaction.getStatus());
	}

	private int findNoTransactionsForIssuer(String issuer, List<Transaction> latestTransactions) {
		int counter = 0;
		for (Transaction transaction : latestTransactions){
			if (issuer.equals(transaction.getMerchant())){
				counter++;
			}
		}
		return counter;
	}

	private double findAmountForAllTransactionsForIssuer(String issuer, List<Transaction> latestTransactions) {
		double amount = 0;
		for (Transaction transaction : latestTransactions){
			if (issuer.equals(transaction.getMerchant())){
				amount+=transaction.getAmount();
			}
		}
		return amount;
	}

	
}
