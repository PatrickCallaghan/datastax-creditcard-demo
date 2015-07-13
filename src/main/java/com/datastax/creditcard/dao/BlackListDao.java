package com.datastax.creditcard.dao;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.creditcard.model.BlacklistMerchant;
import com.datastax.creditcard.model.Transaction;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * Dao class for all the blacklist tables.
 * @author patrickcallaghan
 *
 */
public class BlackListDao {

	private Session session;

	private static Logger logger = LoggerFactory.getLogger(BlackListDao.class);
	private DateFormat dateFormatter = new SimpleDateFormat("yyyyMMdd");
	
	private static String keyspaceName = "datastax_creditcard_demo";
	private static String blacklistMerchants = keyspaceName + ".blacklist_merchants";
	private static String blacklistCards = keyspaceName + ".blacklist_cards";
	private static String blacklistTransactions = keyspaceName + ".blacklist_transactions";

	private static final String GET_ALL_BLACKLIST_TRANSACTIONS = "select * from " + blacklistTransactions
			+ " where date = ?";
	private static final String GET_ALL_BLACKLIST_CARDS = "select * from " + blacklistCards;
	private static final String GET_ALL_BLACKLIST_MERCHANTS = "select * from " + blacklistMerchants
			;
	private static final String INSERT_INTO_BLACKLIST_MERCHANTS = "insert into " + blacklistMerchants
			+ "(issuer, city, amount) values (?,?,?);";
	private static final String INSERT_INTO_BLACKLIST_CARDS = "insert into " + blacklistCards
			+ "(dummy, cc_no, amount) values ('dummy', ?,?);";
	private static final String INSERT_INTO_BLACKLIST_TRANSACTIONS = "insert into " + blacklistTransactions
			+ "(date, transaction_time, transaction_id) values (?,?,?);";
	

	private PreparedStatement insertBlacklistMerchants;
	private PreparedStatement insertBlacklistCards;
	private PreparedStatement insertBlacklistTransactions;
	private PreparedStatement getBlacklistMerchants;
	private PreparedStatement getBlacklistCards;
	private PreparedStatement getBlacklistTransactions;

	public BlackListDao(Session session) {
		this.session = session;
		
		this.insertBlacklistMerchants = session.prepare(INSERT_INTO_BLACKLIST_MERCHANTS);
		this.insertBlacklistCards = session.prepare(INSERT_INTO_BLACKLIST_CARDS);
		this.insertBlacklistTransactions = session.prepare(INSERT_INTO_BLACKLIST_TRANSACTIONS);
		
		this.getBlacklistMerchants = session.prepare(GET_ALL_BLACKLIST_MERCHANTS);
		this.getBlacklistCards = session.prepare(GET_ALL_BLACKLIST_CARDS);
		this.getBlacklistTransactions = session.prepare(GET_ALL_BLACKLIST_TRANSACTIONS);		
	}
	
	public void insertBlacklistMerchant(Date date, String merchant, String city, double amount) {

		session.execute(this.insertBlacklistMerchants.bind(merchant, city, amount));
	}

	public void insertBlacklistCard(Date date, String cc_no, double amount) {

		session.execute(this.insertBlacklistCards.bind(cc_no, amount));
	}

	public void insertBlacklistTransaction(Date date, Transaction transaction) {

		session.execute(this.insertBlacklistTransactions.bind(formatDate(date), transaction.getTransactionTime(), transaction.getTransactionId()));
	}

	public void addInsertBlacklistTransactionToBatch(BatchStatement batch, Date date, Transaction transaction) {

		batch.add(this.insertBlacklistTransactions.bind(formatDate(date), transaction.getTransactionTime(), transaction.getTransactionId()));
	}

	
	public Map<String, Double> getBlacklistCards() {

		ResultSet rs = this.session.execute(this.getBlacklistCards.bind());
		Iterator<Row> iter = rs.iterator();

		Map<String, Double> blacklistCards = new HashMap<String, Double>();

		while (iter.hasNext()) {

			Row row = iter.next();
			blacklistCards.put(row.getString("cc_no"), row.getDouble("amount"));
		}

		return blacklistCards;
	}
	
	public Map<String, BlacklistMerchant> getBlacklistIssuers() {

		ResultSet rs = this.session.execute(this.getBlacklistMerchants.bind());
		Iterator<Row> iter = rs.iterator();

		Map<String, BlacklistMerchant> blacklistIssuers = new HashMap<String, BlacklistMerchant>();

		BlacklistMerchant blacklistIssuer = new BlacklistMerchant();
		Map<String, Double> map = new HashMap<String, Double>();

		while (iter.hasNext()) {

			Row row = iter.next();
			String merchant = row.getString("merchant");

			if (blacklistIssuers.containsKey(merchant)) {
				map = blacklistIssuers.get(merchant).getCityAmount();
			}

			map.put(row.getString("city"), row.getDouble("amount"));
			blacklistIssuer.setCityAmount(map);

			blacklistIssuers.put(merchant, blacklistIssuer);
		}

		return blacklistIssuers;
	}

	public List<String> getBlacklistTransactions(Date date) {
		List<String> transactions = new ArrayList<String>();
		ResultSet rs = this.session.execute(this.getBlacklistTransactions.bind(formatDate(date)));
		Iterator<Row> iter = rs.iterator();

		while (iter.hasNext()) {
			Row row = iter.next();
			transactions.add(row.getString("transaction_id"));
		}

		return transactions;
	}	
	
	private String formatDate(Date date){
		return this.dateFormatter.format(date);
	}
}
