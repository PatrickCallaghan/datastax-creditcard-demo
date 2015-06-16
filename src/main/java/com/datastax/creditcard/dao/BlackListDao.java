package com.datastax.creditcard.dao;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.datastax.creditcard.model.BlacklistIssuer;
import com.datastax.creditcard.model.Transaction;
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

	private DateFormat dateFormatter = new SimpleDateFormat("yyyyMMdd");
	
	private static String keyspaceName = "datastax_creditcard_demo";
	private static String blacklistIssuers = keyspaceName + ".blacklist_issuers";
	private static String blacklistCards = keyspaceName + ".blacklist_cards";
	private static String blacklistTransactions = keyspaceName + ".blacklist_transactions";

	private static final String GET_ALL_BLACKLIST_TRANSACTIONS = "select * from " + blacklistTransactions
			+ " where date = ?";
	private static final String GET_ALL_BLACKLIST_CARDS = "select * from " + blacklistCards;
	private static final String GET_ALL_BLACKLIST_ISSUERS = "select * from " + blacklistIssuers;
	private static final String INSERT_INTO_BLACKLIST_ISSUERS = "insert into " + blacklistIssuers
			+ "(issuer, city, amount) values (?,?,?);";
	private static final String INSERT_INTO_BLACKLIST_CARDS = "insert into " + blacklistCards
			+ "(cc_no, amount) values (?,?);";
	private static final String INSERT_INTO_BLACKLIST_TRANSACTIONS = "insert into " + blacklistTransactions
			+ "(date, transaction_time, transaction_id) values (?,?,?);";
	

	private PreparedStatement insertBlacklistIssuers;
	private PreparedStatement insertBlacklistCards;
	private PreparedStatement insertBlacklistTransactions;
	private PreparedStatement getBlacklistIssuers;
	private PreparedStatement getBlacklistCards;
	private PreparedStatement getBlacklistTransactions;

	public BlackListDao(Session session) {
		this.session = session;
		
		this.insertBlacklistIssuers = session.prepare(INSERT_INTO_BLACKLIST_ISSUERS);
		this.insertBlacklistCards = session.prepare(INSERT_INTO_BLACKLIST_CARDS);
		this.insertBlacklistTransactions = session.prepare(INSERT_INTO_BLACKLIST_TRANSACTIONS);
		
		this.getBlacklistIssuers = session.prepare(GET_ALL_BLACKLIST_ISSUERS);
		this.getBlacklistCards = session.prepare(GET_ALL_BLACKLIST_CARDS);
		this.getBlacklistTransactions = session.prepare(GET_ALL_BLACKLIST_TRANSACTIONS);		
	}
	
	public void insertBlacklistIssuer(Date date, String issuer, String city, double amount) {

		session.execute(this.insertBlacklistIssuers.bind(issuer, city, amount));
	}

	public void insertBlacklistCard(Date date, String cc_no, double amount) {

		session.execute(this.insertBlacklistCards.bind(cc_no, amount));
	}

	public void insertBlacklistTransaction(Date date, Transaction transaction) {

		session.execute(this.insertBlacklistTransactions.bind(formatDate(date), transaction.getTransactionTime(), transaction.getTransactionId()));
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
	
	public Map<String, BlacklistIssuer> getBlacklistIssuers() {

		ResultSet rs = this.session.execute(this.getBlacklistIssuers.bind());
		Iterator<Row> iter = rs.iterator();

		Map<String, BlacklistIssuer> blacklistIssuers = new HashMap<String, BlacklistIssuer>();

		BlacklistIssuer blacklistIssuer = new BlacklistIssuer();
		Map<String, Double> map = new HashMap<String, Double>();

		while (iter.hasNext()) {

			Row row = iter.next();
			String issuer = row.getString("issuer");

			if (blacklistIssuers.containsKey(issuer)) {
				map = blacklistIssuers.get(issuer).getCityAmount();
			}

			map.put(row.getString("city"), row.getDouble("amount"));
			blacklistIssuer.setCityAmount(map);

			blacklistIssuers.put(issuer, blacklistIssuer);
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

	private Transaction rowToTransaction(Row row) {

		Transaction t = new Transaction();
		t.setAmount(row.getDouble("amount"));
		t.setCreditCardNo(row.getString("cc_no"));
		t.setIssuer(row.getString("issuer"));
		t.setItems(row.getMap("items", String.class, Double.class));
		t.setLocation(row.getString("location"));
		t.setTransactionId(row.getUUID("transaction_id").toString());
		t.setTransactionTime(row.getDate("date"));
		t.setNotes(row.getString("notes"));
		t.setStatus(row.getString("status"));

		return t;
	}
}
