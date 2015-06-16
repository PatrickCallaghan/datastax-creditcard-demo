package com.datastax.creditcard;

import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.creditcard.model.Transaction;
import com.datastax.creditcard.services.CreditCardService;
import com.datastax.demo.utils.PropertyHelper;
import com.datastax.demo.utils.Timer;

public class TransactionCreator {

	private static Logger logger = LoggerFactory.getLogger(TransactionCreator.class);
	private DateTime date;
	private static int BATCH = 10000;
	
	private DecimalFormat creditCardFormatter = new DecimalFormat("0000000000000000");
	private static int noOfUsers = 10000000;
	private static int noOfIssuers = 5000000;
	private static int noOfLocations = 10000;
				
	public TransactionCreator() {

		// Create yesterdays date at midnight
		this.date = new DateTime().minusDays(30).withTimeAtStartOfDay();

		String contactPointsStr = PropertyHelper.getProperty("contactPoints", "localhost");
		CreditCardService service = new CreditCardService(contactPointsStr);

		int total = 0;
		long totalTime = 0;
		while (true){
			
			Timer timer = new Timer();
			service.processTransaction(this.createRandomTransaction());
			timer.end();
			
			totalTime = totalTime + timer.getTimeTakenMillis();			
			total ++;
			
			sleep(10);
			if (total > 0 && total % BATCH == 0) {
				total += BATCH;
				logger.info("Wrote " + total + " Transactions at " + totalTime/total + " per ms.");
			}
		}
	}

	private void sleep(int i) {
		try {
			Thread.sleep(i);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private Transaction createRandomTransaction() {

		int creditCardNo = new Double(Math.ceil(Math.random() * noOfUsers)).intValue();
		int noOfItems = new Double(Math.ceil(Math.random() * 5)).intValue();
		
		int issuerNo = new Double(Math.random() * noOfIssuers).intValue();
		int locationNo = new Double(Math.random() * noOfLocations).intValue();
		
		
		String issuerId;
		String location;
		
		if (issuerNo < issuers.size()){
			issuerId = issuers.get(issuerNo);
		}else{
			issuerId = "Issuer" + issuerNo + 1;
		}
		
		if (locationNo < locations.size()){
			location = locations.get(locationNo);
		}else{
			location = "City-" + locationNo + 1;
		}
		
		Transaction transaction = new Transaction();
		createItemsAndAmount(noOfItems, transaction);
		transaction.setCreditCardNo(creditCardFormatter.format(creditCardNo));
		transaction.setIssuer(issuerId);
		transaction.setTransactionId(UUID.randomUUID().toString());
		transaction.setTransactionTime(DateTime.now().toDate());
		transaction.setLocation(location);

		return transaction;
	}

	private void createItemsAndAmount(int noOfItems, Transaction transaction) {
		Map<String, Double> items = new HashMap<String, Double>();
		double totalAmount = 0;

		for (int i = 0; i < noOfItems; i++) {

			double amount = new Double(Math.random() * 1000);
			items.put("item" + i, amount);

			totalAmount += amount;
		}
		transaction.setAmount(totalAmount);
		transaction.setItems(items);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new TransactionCreator();
	}

	private List<String> locations = Arrays.asList("London", "Manchester", "Liverpool", "Glasgow", "Dundee",
			"Birmingham", "New York", "Chicago", "Denver", "Los Angeles", "San Jose", "Santa Clara", "San Fransisco");

	private List<String> issuers = Arrays.asList("Tesco", "Sainsbury", "Asda Wal-Mart Stores", "Morrisons",
			"Marks & Spencer", "Boots", "John Lewis", "Waitrose", "Argos", "Co-op", "Currys", "PC World", "B&Q",
			"Somerfield", "Next", "Spar", "Amazon", "Costa", "Starbucks", "BestBuy", "Wickes", "TFL", "National Rail",
			"Pizza Hut", "Local Pub");	
}
