package com.datastax.creditcard.services;

import java.util.List;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.SolrParams;
import org.joda.time.DateMidnight;

import com.datastax.creditcard.dao.CreditCardDao;
import com.datastax.creditcard.model.Issuer;
import com.datastax.creditcard.model.Transaction;
import com.datastax.creditcard.model.User;

/**
 * 
<p>
Functional requirements
1. Show dashboard with all relevant details of a credit card system.
2. Monitor transactions as they arrive.
3. Monitor transactions by issuer 
4. Create blacklist/watchlist of users and issuers.
5. Raise alerts when a monitor is logged in and transactions in watch list are observed

Dashboard View will contain the following 
1. No of overall transactions
2. No of transactions by sector
3. No of transactions by issuer
4. No of transactions by minute 
5. No of transactions by hour
6. No of transactions by day
7. View of all transactions under review.

User View 
1. Allow searching for a particular user. 
2. Table will contain all transactions for a user, highlighting any under review
3. Ability to approve or reject a transaction
4. View/Add/Delete Users to a blacklist

Issuer View
1. Allow searching for a particular issuer.
2. Table will contain all transactions for am issuer, highlighting any under review
3. Ability to approve or reject a transaction
4. View/Add/Delete Issuers to a blacklist
</p>
 * @author patrickcallaghan
 *
 */
@SuppressWarnings("deprecation")
public class CreditCardService {
	
	private static final int DEFAULT_PAGE = 100;
	private CreditCardDao dao = new CreditCardDao(new String[]{"localhost"});
	private SolrServer solr = new HttpSolrServer("");
	
	public User getUserById(String userId) {
		return dao.getUser(userId);
	}
	
	public User getUserByCCNo(String creditCardNo) throws Exception{
		
		SolrParams params = null;		
		QueryResponse response;
		
		try {
			response = this.solr.query(params);
			return this.getUserById(response.getResponse().get("user_id").toString());
		} catch (SolrServerException e) {
			throw new Exception(e.getMessage());
		}
	}

	public List<Transaction> getTransactions(String creditCardNo) {
		return getTransactions (creditCardNo, -1l, -1l, DEFAULT_PAGE);
	}

	public  List<Transaction> getTransactions(String creditCardNo, long start, long end) {
		return dao.getAllTransactions(creditCardNo, start, end, DEFAULT_PAGE);
	}
	
	public  List<Transaction> getTransactions(String creditCardNo, long start, long end, int defaultPage) {
		return dao.getAllTransactions(creditCardNo, start, end, defaultPage);
	}

	@SuppressWarnings("deprecation")
	public List<Transaction> getIssuerTransactions(String issuer, long date) {
		
		if (date <= 0){			
			date = DateMidnight.now().getMillis();		
		}
				
		return dao.getIssuerTransactions(issuer, date);
	}

	public List<Transaction> getBlacklistTransactions() {
		return dao.getBlacklistTransactions();
	}

	public void updateStatusNotes(String status, String notes, String creditCardNo, String transactionId) {
		dao.updateStatusNotes(status, notes, creditCardNo, transactionId);		
	}

	public int getTotalNoOfTransactions(){
		return 0;
	}
	
	public int getNoOfTransactionsUserMin(){
		return 0;
	}

	public int getNoOfTransactionsUserHour(){
		return 0;
	}

	public int getNoOfTransactionsUserDay(){
		return 0;
	}
	
	public int getNoOfTransactionsUser(){
		return 0;
	}

	public int getNoOfTransactionsIssuerMin(){
		return 0;
	}

	public int getNoOfTransactionsIssuerHour(){
		return 0;
	}

	public int getNoOfTransactionsIssuerDay(){
		return 0;
	}
	
	public int getNoOfTransactionsIssuer(){
		return 0;
	}
	
	public int getNoOfTransactionsSector(){
		return 0;
	}
	
	public List<User> searchUser(){
		return null;
	}


	public List<Issuer> searchIssuer(){
		return null;
	}
}
