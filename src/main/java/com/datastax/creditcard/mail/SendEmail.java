package com.datastax.creditcard.mail;


import java.util.Properties;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.NoSuchProviderException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SendEmail {

	private static Logger logger = LoggerFactory.getLogger(SendEmail.class);
	private final Properties mailServerProperties;
	private Session mailSession;
	private MimeMessage generateMailMessage;
	private Transport transport;


	public SendEmail() throws NoSuchProviderException{
		mailServerProperties = System.getProperties();
		mailServerProperties.put("mail.smtp.port", "587");
		mailServerProperties.put("mail.smtp.auth", "true");
		mailServerProperties.put("mail.smtp.starttls.enable", "true");
		this.mailSession = Session.getDefaultInstance(mailServerProperties, null);
		
		try {
			this.transport = mailSession.getTransport("smtp");
			this.transport.connect("smtp.gmail.com", "creditcard.test2015@gmail.com", "M0nkey2015");
		} catch (NoSuchProviderException e) {
			throw e;
		} catch (MessagingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void generateAndSendEmail(String email, String subject, String message, String linkToConfirm) throws AddressException, MessagingException {

		if (email==null){
			email = "pcallaghan@datastax.com";
		}
		
		generateMailMessage = new MimeMessage(mailSession);
		generateMailMessage.addRecipient(Message.RecipientType.TO, new InternetAddress(email));		
		generateMailMessage.setSubject(subject);	
		generateMailMessage.setContent(message + "<br><br>" + linkToConfirm, "text/html");

		transport.sendMessage(generateMailMessage, generateMailMessage.getAllRecipients());
		logger.info("Message sent to " + email);
	}
	
	
	public static void main(String args[]) throws AddressException, MessagingException {
		SendEmail sendEmail = new SendEmail();
		sendEmail.generateAndSendEmail("pcallaghan@datastax.com", "CreditCardTest","Here's a link <a href='www.google.com'>Google</a>. Thanks.","");
		sendEmail.generateAndSendEmail("pcallaghan@datastax.com", "Another One","Here's a link <a href='www.google.com'>Google</a>. Thanks.","");
	}
}