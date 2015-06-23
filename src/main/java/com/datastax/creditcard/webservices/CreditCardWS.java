package com.datastax.creditcard.webservices;

import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.creditcard.model.Transaction;
import com.datastax.creditcard.model.User;
import com.datastax.creditcard.services.CreditCardService;

@Path("/")
public class CreditCardWS {

	private Logger logger = LoggerFactory.getLogger(CreditCardWS.class);

	//Service Layer.
	private CreditCardService service = new CreditCardService();

	@GET
	@Path("/get/user")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getUser(@QueryParam("userId") String userId) {
				
		User user = service.getUserById(userId);

		return Response.status(201).entity(user).build();
	}

	@GET
	@Path("/get/userbycard")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getUserByCardNo(@QueryParam("creditCardNo") String creditCardNo) {
				
		try {
			User user = null;// = service.getUserByCCNo(creditCardNo);
			
			return Response.status(201).entity(user).build();
		} catch (Exception e) {
			return Response.status(201).entity(e.getMessage()).build();
		}

	}
	
	@GET
	@Path("/get/transactions")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getTransactions(@QueryParam("creditCardNo") String creditCardNo, 
			@QueryParam("start") long start, @QueryParam("end") long end) {
		
		List<Transaction> transactions = this.service.getTransactions(creditCardNo, start, end);
		
		return Response.status(201).entity(transactions).build();
	}
	
	@GET
	@Path("/get/issuer/transactions")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getIssuerTransactions(@QueryParam("issuer") String issuer, @QueryParam("date") long date) {
		
		List<Transaction> transactions = this.service.getIssuerTransactions(issuer, date);
		
		return Response.status(201).entity(transactions).build();
	}

	@GET
	@Path("/get/transaction/blacklist")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getBlacklistTransactions() {
		
		List<Transaction> transactions = this.service.getBlacklistTransactions();
		
		return Response.status(201).entity(transactions).build();
	}
	
	@POST
	@Path("/post/update/transaction")
	@Produces(MediaType.APPLICATION_JSON)
	public Response updateStatusNotes(@QueryParam("status") String status, @QueryParam("notes") String notes,
			@QueryParam("transactionId") String transactionId) {
		
		this.service.updateStatusNotes(status, notes, transactionId);
		
		return Response.status(201).build();
	}	
}
