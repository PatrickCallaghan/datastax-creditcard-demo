Credit Card Demo
====================

Fraud Detection Dashboard

The Fraud Detection systems encapsulates a credit card transaction system and dashboard for monitoring fraudulent transactions and suspicious issuers and users. This system will also allow a monitor to approve or reject a transaction in realtime.



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
2. Table will contain all transactions for a user, 	highlighting any under review
3. Ability to approve or reject a transaction
4. View/Add/Delete Users to a blacklist

Issuer View
1. Allow searching for a particular issuer.
2. Table will contain all transactions for am issuer, highlighting any under review
3. Ability to approve or reject a transaction
4. View/Add/Delete Issuers to a blacklist

Spark Jobs need to created for some analysis per day of transactions.

Search needs to able to look up issuers and users.

Trade Processor
Generally 10 transactions per second which is nearly 1 million a day

Trade processor will includes spikes every 10 days or so to simulate busy days





Commands

~/Tools/cassandra-loader/build/cassandra-loader -f users.csv -host localhost -schema "datastax_creditcard_demo.users(user_id,first,last,gender,city,state,cc_no)"

~/Tools/cassandra-loader/build/cassandra-loader -f issuers.csv -host localhost -schema "datastax_creditcard_demo.issuers(id, name, location)"

insert into blacklist_issuers (issuer, city, amount) values ('Issuer7','City-4986',450);
insert into blacklist_issuers (issuer, city, amount) values ('Issuer7','City-4986',50);
insert into blacklist_issuers (issuer, city, amount) values ('Issuer50000','City-1969',200);

insert into blacklist_cards (dummy, cc_no, amount) values ('dummy', '0000000005702649', 1000);
insert into blacklist_cards (dummy, cc_no, amount) values ('dummy', '0000000004737244', 1000);
