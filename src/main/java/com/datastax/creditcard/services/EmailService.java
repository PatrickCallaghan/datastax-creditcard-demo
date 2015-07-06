package com.datastax.creditcard.services;

import javax.mail.MessagingException;
import javax.mail.NoSuchProviderException;
import javax.mail.internet.AddressException;

import com.datastax.creditcard.mail.SendEmail;
import com.datastax.creditcard.model.Transaction;
import com.datastax.creditcard.model.User;

public class EmailService {

	private static final String CONFIRMATION_URL = "http://localhost:9999/datastax-creditcard-webservice/rest/user/confirmtransaction?transactionId=";
	private SendEmail sendEmail;
	
	public EmailService(){
		try {
			this.sendEmail = new SendEmail();
		} catch (NoSuchProviderException e) {
			throw new RuntimeException(e);
		}
	}

	public void sendClientEmail(User user, Transaction transaction) {
		
		String subject = "Transaction for card no " + transaction.getCreditCardNo() + " for " + transaction.getAmount() +" at " + transaction.getIssuer() + " needs approval.";
		String message = "Please click on the link below to approve the following Transaction. <br><br>" + transaction.toString() + "<br>";
		String linkToConfirm = CONFIRMATION_URL + transaction.getTransactionId();

		try {
			sendEmail.generateAndSendEmail(user.getEmail(), subject, message, linkToConfirm);
		} catch (AddressException e) {
			e.printStackTrace();
		} catch (MessagingException e) {
			e.printStackTrace();
		}
	}
}
