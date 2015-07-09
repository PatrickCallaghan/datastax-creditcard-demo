package com.datastax.creditcard.services;

import javax.mail.MessagingException;
import javax.mail.NoSuchProviderException;
import javax.mail.internet.AddressException;

import com.datastax.creditcard.mail.SendEmail;
import com.datastax.creditcard.model.Transaction;
import com.datastax.creditcard.model.User;

public class EmailService {

	private static final String CONFIRMATION_URL = "http://localhost:9999/datastax-creditcard-webservice/rest/user/confirmtransaction?transactionId=";	
	private static final String DECLINE_URL = "http://localhost:9999/datastax-creditcard-webservice/rest/user/declinetransaction?transactionId=";
	private SendEmail sendEmail;
	
	public EmailService(){
		try {
			this.sendEmail = new SendEmail();
		} catch (NoSuchProviderException e) {
			throw new RuntimeException(e);
		}
	}

	public void sendClientEmail(final User user, final Transaction transaction) {
		
		Runnable r  = new Runnable(){

			@Override
			public void run() {
				String subject = "Transaction for card no " + transaction.getCreditCardNo() + " for " + transaction.getAmount() +" at " + transaction.getIssuer() + " needs approval.";
				String message = "Please click on the links below to approve/decline the following Transaction. <br><br>" + transaction.toString() + "<br>";
				String linkToConfirm = "<a href='" + CONFIRMATION_URL + transaction.getTransactionId() +"'>Confirm</a><br><br><a href='" + DECLINE_URL + transaction.getTransactionId() + "'>Decline</a>";		

				try {
					sendEmail.generateAndSendEmail(user.getEmail(), subject, message, linkToConfirm);
				} catch (AddressException e) {
					e.printStackTrace();
				} catch (MessagingException e) {
					e.printStackTrace();
				}
			}			
		};
		
		new Thread(r).start();		
	}
}
