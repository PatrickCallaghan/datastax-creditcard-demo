package com.datastax.creditcard.dao;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.creditcard.model.CreditBalance;
import com.datastax.creditcard.model.Transaction;
import com.datastax.creditcard.model.User;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;

public class CreditCardDao {

	private static Logger logger = LoggerFactory.getLogger(CreditCardDao.class);
	private static final int DEFAULT_LIMIT = 100;
	private Session session;

	private DateFormat dateFormatter = new SimpleDateFormat("yyyyMMdd");
	private static String keyspaceName = "datastax_creditcard_demo";
	private static String balanceTable = keyspaceName + ".credit_card_transactions_balance";
	private static String issuerTable = keyspaceName + ".credit_card_transactions_by_issuer_date";

	private static String transactionTable = keyspaceName + ".transactions";
	private static String latestTransactionTable = keyspaceName + ".latest_transactions";
	private static String transactionCounterTable = keyspaceName + ".transaction_date_minute_counter";
	private static String userTable = keyspaceName + ".users";

	private static final String GET_TRANSACTIONS_CCNO_AND_ID = "select transaction_id from " + balanceTable
			+ " where cc_no = ?";

	private static final String INSERT_INTO_TRANSACTION = "Insert into " + transactionTable
			+ " (cc_no, transaction_id, items, location, issuer, amount, notes) values (?,now(),?,?,?,?,?);";
	private static final String INSERT_INTO_LATEST_TRANSACTION = "Insert into " + latestTransactionTable
			+ " (cc_no, transaction_id, items, location, issuer, amount, notes) values (?,now(),?,?,?,?,?)";

	private static final String INSERT_INTO_ISSUER = "insert into " + issuerTable
			+ "(issuer, date, transaction_id, cc_no, amount) values (?,?, now(),?,?)";

	private static final String UPDATE_COUNTER = "update " + transactionCounterTable + " "
			+ "set  total_for_minute = total_for_minute + 1 where date=? AND hour=? AND minute=?";

	private static final String UPDATE_BALANCE = "update " + balanceTable
			+ " set balance = ?, balance_at = ?  where cc_no = ?";

	private static final String UPDATE_TRANSACTION_STATUS = "update " + transactionTable
			+ " set status = ?, notes = ? where transaction_id = ?";
	private static final String UPDATE_LATEST_TRANSACTION_STATUS = "update " + latestTransactionTable
			+ " set status = ?, notes = ? where cc_no = ? and transaction_id = ?";

	private static final String INSERT_BALANCE = "insert into " + balanceTable
			+ "(cc_no, balance, balance_at) values (?,?,?)";

	private static final String GET_ALL_ISSUER_BY_DATE = "select transaction_id,amount,cc_no   from " + issuerTable
			+ " where issuer = ? and date = ?";
	private static final String GET_ALL_TRANSACTIONS_BY_TIME = "select transaction_id,amount,cc_no from "
			+ balanceTable + " where cc_no = ? and transaction_id > maxTimeuuid(?) and transaction_id < minTimeuuid(?) LIMIT ?";
	private static final String GET_TRANSACTIONS_BY_ID = "select dateof(transaction_id) as date,transaction_id,amount,cc_no,location,items,issuer,status,notes from "
			+ transactionTable + " where transaction_id = ?";

	private static final String GET_ALL_CREDIT_CARDS = "select cc_no, balance_at, balance from " + balanceTable;
	private static final String GET_USER = "Select * from " + userTable + " where user_id = ?";

	private PreparedStatement insertTransactionStmt;
	private PreparedStatement insertLatestTransactionStmt;
	private PreparedStatement insertIssuerStmt;
	private PreparedStatement updateCounter;
	private PreparedStatement updateBalance;
	private PreparedStatement insertBalance;
	private PreparedStatement getTransTime;
	private PreparedStatement getUser;
	private PreparedStatement getIssuerTransactionsByDate;
	private PreparedStatement updateTransactionStatus;
	private PreparedStatement getTransaction;
	private PreparedStatement getTransactionById;
	private PreparedStatement updateLatestTransactionStatus;
	
	private CounterDao counterDao;
	private BlackListDao blackListDao;

	public CreditCardDao(String[] contactPoints) {

		Cluster cluster = Cluster.builder().addContactPoints(contactPoints).build();

		this.session = cluster.connect();
		
		counterDao = new CounterDao(session);
		blackListDao = new BlackListDao(session);

		try {
			this.insertTransactionStmt = session.prepare(INSERT_INTO_TRANSACTION);
			this.insertLatestTransactionStmt = session.prepare(INSERT_INTO_LATEST_TRANSACTION);
			this.insertIssuerStmt = session.prepare(INSERT_INTO_ISSUER);
			this.updateCounter = session.prepare(UPDATE_COUNTER);

			this.updateBalance = session.prepare(UPDATE_BALANCE);
			this.insertBalance = session.prepare(INSERT_BALANCE);
			this.getTransTime = session.prepare(GET_ALL_TRANSACTIONS_BY_TIME);
			this.getUser = session.prepare(GET_USER);
			this.getIssuerTransactionsByDate = session.prepare(GET_ALL_ISSUER_BY_DATE);


			this.updateTransactionStatus = session.prepare(UPDATE_TRANSACTION_STATUS);
			this.updateLatestTransactionStatus = session.prepare(UPDATE_LATEST_TRANSACTION_STATUS);
			this.getTransaction = session.prepare(GET_TRANSACTIONS_CCNO_AND_ID);
			this.getTransactionById = session.prepare(GET_TRANSACTIONS_BY_ID);

			this.insertLatestTransactionStmt.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
			this.insertTransactionStmt.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
			this.insertIssuerStmt.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
			this.updateCounter.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

			this.updateBalance.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
			this.getTransTime.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
			this.getUser.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
			this.getIssuerTransactionsByDate.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
			
		} catch (Exception e) {
			e.printStackTrace();
			session.close();
			cluster.close();
		}
	}

	public void createCreditCards(int cards) {

		for (int i = 1; i < cards + 1; i++) {
			session.execute(this.insertBalance.bind("" + i, 0d, DateTime.parse("2001-01-01").toDate()));
		}
	}

	public void updateStatusNotes(String status, String notes, String creditCardNo, String transactionId) {

		Transaction transaction = this.getTransaction(creditCardNo, transactionId);

		BatchStatement batch = new BatchStatement();
		batch.add(updateTransactionStatus.bind(status, notes, transaction.getCreditCardNo(),
				transaction.getTransactionId()));
		batch.add(updateLatestTransactionStatus.bind(status, notes, transaction.getCreditCardNo(),
				transaction.getTransactionId()));
		session.execute(batch);
	}

	public void insertTransaction(Transaction transaction) {

		DateTime dateTime = new DateTime(transaction.getTransactionTime());

		int minute = dateTime.getMinuteOfHour();
		int hour = dateTime.getHourOfDay();
		String date = dateFormatter.format(dateTime.toDate());

		BatchStatement batch = new BatchStatement();

		batch.add(this.insertTransactionStmt.bind(transaction.getCreditCardNo(), transaction.getTransactionTime(),
				transaction.getTransactionId(), transaction.getItems(), transaction.getLocation(),
				transaction.getIssuer(), transaction.getAmount()));
		batch.add(this.insertIssuerStmt.bind(transaction.getIssuer(), date, transaction.getTransactionId(),
				transaction.getCreditCardNo(), transaction.getTransactionTime(), transaction.getAmount()));

		session.execute(batch);

		session.execute(this.updateCounter.bind(date, hour, minute));
	}

	public User getUser(String userId) {

		ResultSet rs = this.session.execute(this.getUser.bind(userId));
		Row row = rs.one();

		return rowToUser(row);
	}

	private User rowToUser(Row row) {
		User user = new User();
		user.setUserId(row.getString("user_id"));
		user.setCityName(row.getString("city"));
		user.setCreditCardNo(row.getString("cc_no"));
		user.setFirstname(row.getString("first"));
		user.setGender(row.getString("gender"));
		user.setLastname(row.getString("last"));
		user.setStateName(row.getString("state"));
		return user;
	}

	public boolean updateCreditCardWithBalance() {
		// Get all credit cards
		ResultSet resultSetAllCreditCards = session.execute(GET_ALL_CREDIT_CARDS);
		List<CreditBalance> creditBalances = this.createCreditBalances(resultSetAllCreditCards);
		Date balanceAt;

		logger.info("Updating balances for " + creditBalances.size() + " credit cards.");

		for (CreditBalance creditBalance : creditBalances) {

			balanceAt = creditBalance.getBalanceAt() == null ? DateTime.parse("2001-01-01").toDate() : creditBalance
					.getBalanceAt();
			double total = creditBalance.getAmount() == null ? 0 : creditBalance.getAmount();

			// set the new balance date to the last one until we get new
			// transactions.
			Date newBalanceAt = balanceAt;
			ResultSet resultSet = session.execute(this.getTransTime.bind(creditBalance.getCreditCardNo(), balanceAt));
			Iterator<Row> iterator = resultSet.iterator();

			while (iterator.hasNext()) {
				Row row = iterator.next();

				total = total + row.getDouble("amount");

				if (row.getDate("transaction_time").after(newBalanceAt)) {
					newBalanceAt = row.getDate("transaction_time");
				}
			}

			// update the balance with the date of the latest transaction that
			// has been processed.
			session.execute(updateBalance.bind(total, newBalanceAt, creditBalance.getCreditCardNo()));
		}

		return true;
	}

	private List<CreditBalance> createCreditBalances(ResultSet resultSet) {

		List<CreditBalance> creditBalances = new ArrayList<CreditBalance>();
		Iterator<Row> iterator = resultSet.iterator();

		while (iterator.hasNext()) {
			Row row = iterator.next();

			creditBalances.add(new CreditBalance(row.getString("cc_no"), row.getDate("balance_at"), row
					.getDouble("balance")));
		}
		return creditBalances;
	}

	public List<Transaction> getAllTransactions(String creditCardNo, long start, long end, int limit) {

		if (start < 0)
			start = 0;
		if (end < 0)
			end = new Date().getTime();
		if (limit < 0)
			limit = DEFAULT_LIMIT;

		List<Transaction> transactions = new ArrayList<Transaction>();
		ResultSet rs = this.session.execute(this.getTransTime.bind(creditCardNo, new Date(start), new Date(end), limit));
		
		Iterator<Row> iter = rs.iterator();

		while (iter.hasNext()) {

			Row row = iter.next();
			transactions.add(rowToTransaction(row));
		}

		return transactions;
	}

	private Transaction getTransaction(String creditCardNo, String transactionId) {

		ResultSet rs = this.session.execute(this.getTransaction.bind(creditCardNo, transactionId));

		return rowToTransaction(rs.one());
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

	public List<Transaction> getIssuerTransactions(String issuer, long date) {

		List<Transaction> transactions = new ArrayList<Transaction>();
		ResultSet rs = this.session.execute(this.getIssuerTransactionsByDate.bind(issuer,
				dateFormatter.format(new Date(date))));
		Iterator<Row> iter = rs.iterator();

		while (iter.hasNext()) {

			Row row = iter.next();
			transactions.add(rowToTransaction(row));
		}

		return transactions;
	}



	public List<Transaction> getTransactions(List<UUID> transactionIds) {

		List<Transaction> transactions = new ArrayList<Transaction>();
		List<ResultSetFuture> results = new ArrayList<ResultSetFuture>();

		for (UUID transactionId : transactionIds) {

			results.add(this.session.executeAsync(getTransactionById.bind(transactionId)));
		}

		for (ResultSetFuture future : results) {
			transactions.add(this.rowToTransaction(future.getUninterruptibly().one()));
		}

		return transactions;
	}


	public List<String> getCreditCardNosIter() {
		
		Statement stmt = new SimpleStatement ("select * from " + userTable);
		ResultSet rs = this.session.execute(stmt);		
		Iterator<Row> iter = rs.iterator();
		
		List<String> creditCardNos = new ArrayList<String>();
		
		int count=0;
		
		while (iter.hasNext()){
			
			Row row = iter.next();
			creditCardNos.add(row.getString("cc_no"));
			count++;
		}
		
		logger.info("Count (iter): " + count);
		return creditCardNos;
	}

	//Delegated methods
	public List<Transaction> getBlacklistTransactions() {
		return this.blackListDao.getBlacklistTransactions();
	}
	
	public void insertBlacklistIssuer(String issuer, String city, double amount) {
		this.blackListDao.insertBlacklistIssuer(issuer, city, amount);
	}

	public void insertBlacklistCard(String cc_no, double amount) {
		this.blackListDao.insertBlacklistCard(cc_no, amount);
	}

}
