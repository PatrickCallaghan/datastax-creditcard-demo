package com.datastax.creditcard.services;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.creditcard.model.Transaction;
import com.datastax.creditcard.model.UserRule;
import com.datastax.creditcard.model.Transaction.Status;

public class UserRuleService {
	
	private static Logger logger = LoggerFactory.getLogger(UserRuleService.class);
	
	public Transaction.Status processUserRule(Transaction transaction, UserRule userRule, List<Transaction> latestTransactions){
		
		//Issuers don't match
		if (!userRule.getIssuer().equals(transaction.getIssuer())){
			return Status.APPROVED;
		}
		
		if (userRule.getAmount() > 0){

			String issuer = userRule.getIssuer();
			double totalAmountForIssuer = this.findAmountForAllTransactionsForIssuer(issuer, latestTransactions);
			totalAmountForIssuer = totalAmountForIssuer + transaction.getAmount();
	
			logger.info("Total amount for " + issuer + " will be " + totalAmountForIssuer);
			
			if (totalAmountForIssuer > userRule.getAmount()){
				transaction.setNotes("Total Amount is " + totalAmountForIssuer + " for issuer " + issuer + "." + userRule.getRuleName() + " needs approval for any more transactions.");
				transaction.setStatus(Status.CLIENT_APPROVAL.toString());
				return Status.CLIENT_APPROVAL;
			}

		}else if (userRule.getNoOfTransactions() > 0){
			
			String issuer = userRule.getIssuer();
			int noOfTransactions = findNoTransactionsForIssuer(issuer, latestTransactions);
	
			if (noOfTransactions > userRule.getNoOfTransactions()){
				transaction.setNotes("No of transaction is " + noOfTransactions + " for issuer " + issuer + "." + userRule.getRuleName() + " needs approval for more than " + userRule.getNoOfTransactions() + " transactions");
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
			if (issuer.equals(transaction.getIssuer())){
				counter++;
			}
		}
		return counter;
	}

	private double findAmountForAllTransactionsForIssuer(String issuer, List<Transaction> latestTransactions) {
		double amount = 0;
		for (Transaction transaction : latestTransactions){
			if (issuer.equals(transaction.getIssuer())){
				amount+=transaction.getAmount();
			}
		}
		return amount;
	}

	
}
