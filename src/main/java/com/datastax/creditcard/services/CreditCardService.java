package com.datastax.creditcard.services;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.joda.time.DateMidnight;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.creditcard.dao.CreditCardDao;
import com.datastax.creditcard.model.BlacklistMerchant;
import com.datastax.creditcard.model.Merchant;
import com.datastax.creditcard.model.Transaction;
import com.datastax.creditcard.model.Transaction.Status;
import com.datastax.creditcard.model.User;
import com.datastax.creditcard.model.UserRule;
import com.datastax.creditcard.rules.DuplicateTransactionRule;
import com.datastax.creditcard.rules.MerchantBlackListRule;
import com.datastax.creditcard.rules.Rule;
import com.datastax.creditcard.rules.TransactionAmountRule;
import com.datastax.demo.utils.PropertyHelper;
import com.datastax.demo.utils.Timer;

/**
 * 
 <p>
 * Functional requirements 1. Show dashboard with all relevant details of a
 * credit card system. 2. Monitor transactions as they arrive. 3. Monitor
 * transactions by issuer 4. Create blacklist/watchlist of users and issuers. 5.
 * Raise alerts when a monitor is logged in and transactions in watch list are
 * observed
 * 
 * Dashboard View will contain the following 1. No of overall transactions 2. No
 * of transactions by sector 3. No of transactions by issuer 4. No of
 * transactions by minute 5. No of transactions by hour 6. No of transactions by
 * day 7. View of all transactions under review.
 * 
 * User View 1. Allow searching for a particular user. 2. Table will contain all
 * transactions for a user, highlighting any under review 3. Ability to approve
 * or reject a transaction 4. View/Add/Delete Users to a blacklist
 * 
 * Issuer View 1. Allow searching for a particular issuer. 2. Table will contain
 * all transactions for am issuer, highlighting any under review 3. Ability to
 * approve or reject a transaction 4. View/Add/Delete Issuers to a blacklist
 * </p>
 * 
 * @author patrickcallaghan
 *
 */
@SuppressWarnings("deprecation")
public class CreditCardService {

	private static final int SOLR_PAGE_SIZE = 100;

	private static Logger logger = LoggerFactory.getLogger(CreditCardService.class);

	private static final int DEFAULT_PAGE = 100;
	private CreditCardDao dao;
	private EmailService emailService;
	private UserRuleService userRuleService;
	
	private SolrServer solr;
	private List<Rule> rules = new ArrayList<Rule>();

	private Map<String, String> ccNoUserIdMap = new HashMap<String, String>();
	private Map<String, Double> ccNoBlackMap = new HashMap<String, Double>();
	private Map<String, BlacklistMerchant> merchantBlackList = new HashMap<String, BlacklistMerchant>();

	private Rule merchantBlackListRule = new MerchantBlackListRule();
	private Rule transactionAmountRule = new TransactionAmountRule();
	private Rule duplicateTransactionRule = new DuplicateTransactionRule();

	public CreditCardService() {
		
		String contactPointsStr = PropertyHelper.getProperty("contactPoints", "localhost");		
		this.dao = new CreditCardDao(contactPointsStr.split(","));
		
		solr = new HttpSolrServer("http://"+contactPointsStr+":8983/solr/datastax_creditcard_demo.users");
		 
		init();
	}

	public CreditCardService(String contactPointsStr) {
		this.dao = new CreditCardDao(contactPointsStr.split(","));
		init();
	}

	private void init() {
		this.rules = new ArrayList<Rule>();
		this.rules.add(merchantBlackListRule);
		this.rules.add(transactionAmountRule);
		this.rules.add(duplicateTransactionRule);
		
		this.emailService = new EmailService();
		this.userRuleService = new UserRuleService();

		startScheduler();
	}

	public void loadRefData() {
		logger.info("Loading Credit Card-UserId Map");
		Timer timer = new Timer();
		ccNoUserIdMap = dao.getCreditCardUserIdMap();
		timer.end();
		logger.info("Loaded Credit Card-UserId Map of size : " + ccNoUserIdMap.size() + " in "
				+ timer.getTimeTakenSeconds() + "secs");
	}

	private void startScheduler() {
		// Schedule the update blacklists
		ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();

		exec.scheduleWithFixedDelay(new Runnable() {

			@Override
			public void run() {
				Timer timer = new Timer();
				ccNoBlackMap = dao.getBlackListDao().getBlacklistCards();
				merchantBlackList = dao.getBlackListDao().getBlacklistIssuers();
				timer.end();

				for (Rule rule : rules) {
					rule.setCCNoBlackList(ccNoBlackMap);
					rule.setIssuerBlackList(merchantBlackList);
				}
			}

		}, 0, 1, TimeUnit.SECONDS);
	}

	public Transaction checkTransaction(Transaction transaction) {

		List<UserRule> userRules = this.dao.getUserRulesDao().getAllUserRules(transaction.getUserId());
		
		Timer timer = new Timer();
		processRules(transaction, userRules);
		timer.end();

		return transaction;
	}

	public String getUserIdFromCCNo(String ccNo) {
		return this.ccNoUserIdMap.get(ccNo);
	}
	
	public void confirmTransaction(String transactionId) {
		this.updateStatusNotes(Status.CLIENT_APPROVED.toString(), "Client Approved by WS at " + new Date().toString(), transactionId);
	}
	
	public void declineTransaction(String transactionId) {
		this.updateStatusNotes(Status.DECLINED.toString(), "Client Declined by WS at " + new Date().toString(), transactionId);
	}


	public Transaction processTransaction(Transaction transaction) {
		
		if(!validate(transaction)){
			return transaction;
		}
		
		List<UserRule> userRules = this.dao.getUserRulesDao().getAllUserRules(transaction.getUserId());
		
		if (!transaction.getStatus().equals(Status.CLIENT_APPROVED.toString())) {
			Timer timer = new Timer();
			processRules(transaction, userRules);
			timer.end();
		}

		if (transaction.getStatus().equals(Status.CLIENT_APPROVAL.toString())) {
			User user = this.getUserById(transaction.getUserId());
			this.emailService.sendClientEmail(user, transaction);
		}

		dao.saveTransaction(transaction);
		dao.getCounterDao().updateCounters(transaction, transaction.getUserId());

		return transaction;
	}


	private boolean validate(Transaction transaction) {
		if (transaction.getUserId() == null || transaction.getUserId().equals(""))
			return false;
		
		return true;
	}

	private void processRules(Transaction transaction, List<UserRule> userRules) {
		
		//Always going to get the last 7 days for the demo
		List<Transaction> latestTransactions = this.getLatestTransactions(transaction.getCreditCardNo());		
		
		for (UserRule userRule : userRules){			
			Status status = this.userRuleService.processUserRule(transaction, userRule, latestTransactions);
			if (!status.equals(Status.APPROVED)) {
				break;
			}
 		}
		
		//Failed userRules - return;
		if (!transaction.getStatus().equals(Status.APPROVED.toString())) {
			return;
		}
		
		for (Rule rule : rules ){
			rule.setLatestTransactions(latestTransactions);
			
			Status status = rule.processRule(transaction);			
			
			if (!status.equals(Status.APPROVED)) {
				break;
			}
		}		
	}
	
	public void insertUserRule(UserRule userRule){
		this.dao.getUserRulesDao().insertUserRule(userRule);
	}
	
	public void deleteUserRule(String userId, String ruleId){
		this.dao.getUserRulesDao().deleteUserRule(userId, ruleId);
	}
	
	public List<UserRule> getUserRules(String userId){
		return this.dao.getUserRulesDao().getAllUserRules(userId);
	}

	public UserRule getUserRule(String userId, String ruleId){
		return this.dao.getUserRulesDao().getUserRule(userId, ruleId);
	}
	
	public void insertBlacklistMerchant(Date date, String merchant, String city, double amount) {

		this.dao.getBlackListDao().insertBlacklistMerchant(date, merchant, city, amount);
	}

	public void insertBlacklistCard(Date date, String cc_no, double amount) {

		this.dao.getBlackListDao().insertBlacklistCard(date, cc_no, amount);
	}

	public void insertBlacklistTransaction(Date date, Transaction transaction) {

		this.dao.getBlackListDao().insertBlacklistTransaction(date, transaction);
	}

	public User getUserById(String userId) {
		return dao.getUser(userId);
	}

	public Merchant getMerchantById(String merchantId) {
		return dao.getMerchant(merchantId);
	}

	public Transaction getTransaction(String transactionId) {
		return dao.getTransaction(transactionId);
	}

	public List<Transaction> getTransactions(String creditCardNo) {
		return getTransactions(creditCardNo, -1l, -1l, DEFAULT_PAGE);
	}

	public List<Transaction> getTransactions(String creditCardNo, long start, long end) {
		return dao.getAllTransactions(creditCardNo, start, end, DEFAULT_PAGE);
	}

	public List<Transaction> getTransactions(String creditCardNo, long start, long end, int defaultPage) {
		return dao.getAllTransactions(creditCardNo, start, end, defaultPage);
	}

	@SuppressWarnings("deprecation")
	public List<Transaction> getMerchantTransactions(String issuer, long date) {

		if (date <= 0) {
			date = DateMidnight.now().getMillis();
		}

		return dao.getMerchantTransactions(issuer, date);
	}

	public List<Transaction> getBlacklistTransactions() {
		return this.getBlacklistTransactions(new Date());
	}

	public List<Transaction> getLatestTransactions(String ccNo) {
		return this.dao.getLatestTransactionsForCCNo(ccNo);
	}

	public List<Transaction> getBlacklistTransactions(Date date) {

		return this.dao.getTransactions(dao.getBlackListDao().getBlacklistTransactions(date));
	}

	public void updateStatusNotes(String status, String notes, String transactionId) {
		dao.updateStatusNotes(status, notes, transactionId);
	}

	public Map<String, Long> getTotalNoOfTransactions(int nDays) {
		return dao.getCounterDao().getTransCounterLastNDays(DateTime.now(), nDays);
	}

	public Map<String, Integer> getNoOfTransactionsUserHour(String user, int hour, int nDays) {
		return dao.getCounterDao().getUserHourCounterLastNDays(user, DateTime.now(), hour, nDays);
	}

	public Map<String, Integer> getNoOfTransactionsUserDay(String user, int nDays) {
		return dao.getCounterDao().getUserCounterLastNDays(user, DateTime.now(), nDays);
	}

	public Map<String, Integer> getNoOfTransactionsIssuerHour(String issuer, int hour, int nDays) {
		return dao.getCounterDao().getUserHourCounterLastNDays(issuer, DateTime.now(), hour, nDays);
	}

	public Map<String, Integer> getNoOfTransactionsIssuerDay(String issuer, int nDays) {
		return dao.getCounterDao().getMerchantCounterLastNDays(issuer, DateTime.now(), nDays);
	}

	public List<User> searchUser(String field, String filterValue) {

		List<User> users = new ArrayList<User>();
		
		logger.info("Searching for " + String.format("%1$s:*%2$s*", field, filterValue));
				
		SolrQuery parameters = new SolrQuery();
		parameters.set("q", String.format("%1$s:*%2$s*", field, filterValue));
		parameters.set("start", 0);
		parameters.set("rows", SOLR_PAGE_SIZE);

		QueryResponse response;
		try {
			response = solr.query(parameters);

			SolrDocumentList results = response.getResults();

			for (int i = 0; i < results.size(); ++i) {
				System.out.println(results.get(i));

				SolrDocument solrDocument = results.get(i);
				Map<String, Object> fieldValueMap = solrDocument.getFieldValueMap();

				User user = new User();
				user.setUserId((String) fieldValueMap.get("user_id"));
				user.setCityName((String) fieldValueMap.get("city"));
				user.setStateName((String) fieldValueMap.get("state"));
				user.setCreditCardNo((String) fieldValueMap.get("cc_no"));
				user.setFirstname((String) fieldValueMap.get("first"));
				user.setLastname((String) fieldValueMap.get("last"));
				user.setGender((String) fieldValueMap.get("gender"));
				
				users.add(user);
			}
		} catch (SolrServerException e) {
			e.printStackTrace();
		}

		logger.info("Returning " + users.size() + " users");
		
		return users;
	}

	public List<Merchant> searchIssuer() {
		return null;
	}

	public static void main(String args[]) {
		CreditCardService service = new CreditCardService();
		logger.info(service.getTotalNoOfTransactions(10).toString());
		service.searchUser("cc_no", "*00000102*");
	}	
}
