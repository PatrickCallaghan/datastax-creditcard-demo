<<<<<<< HEAD
create keyspace if not exists datastax_creditcard_demo WITH replication = {'class': 'NetworkTopologyStrategy', 'SearchAnalytics': 1};
=======
create keyspace if not exists datastax_creditcard_demo WITH replication = {'class': 'NetworkTopologyStrategy', 'Analytics': 1};
>>>>>>> 59a81d5e776e7631546539816978c1e28024a2e8

use datastax_creditcard_demo; 
 
CREATE TABLE if not exists credit_card_transactions_balance (
	cc_no text,
<<<<<<< HEAD
	transaction_time timestamp,
	transaction_id text,
	amount double,
	balance_at timestamp static,
	balance double static,
	PRIMARY KEY (cc_no, transaction_time )
) WITH CLUSTERING ORDER BY (transaction_time DESC)
AND compaction = {'class': 'DateTieredCompactionStrategy'};

CREATE TABLE if not exists credit_card_transactions_by_issuer_date (
	issuer text,
	transaction_time timestamp,
	date text, 
	transaction_id text,
	cc_no text,
	amount double,
 	PRIMARY KEY ((issuer, date), transaction_time)
) WITH CLUSTERING ORDER BY (transaction_time DESC)
AND compaction = {'class': 'DateTieredCompactionStrategy'};
	
create table if not exists latest_transactions(
	cc_no text,
	transaction_time timestamp,
	transaction_id text,
=======
	transaction_id timeuuid,
	amount double,
	balance_at timestamp static,
	balance double static,
	PRIMARY KEY (cc_no, transaction_id)
) WITH CLUSTERING ORDER BY (transaction_id DESC);

CREATE TABLE if not exists credit_card_transactions_by_issuer_date (
	issuer text,
	date text, 
	transaction_id timeuuid,
	cc_no text,
	amount double,
 	PRIMARY KEY ((issuer, date), transaction_id)
) WITH CLUSTERING ORDER BY (transaction_id DESC);
	
create table if not exists latest_transactions(
	cc_no text,
	transaction_id timeuuid,
>>>>>>> 59a81d5e776e7631546539816978c1e28024a2e8
	location text,
	items map<text, double>,
	issuer text,
	amount double,
<<<<<<< HEAD
	user_id text,
	status text,
	notes text,
	PRIMARY KEY (cc_no, transaction_id)
)  WITH compaction = {'class': 'DateTieredCompactionStrategy', 'max_sstable_age_days':'3'}
	AND default_time_to_live = 604800;

create table if not exists transactions(
	cc_no text,
	transaction_time timestamp,
 	transaction_id text,
 	user_id text,
=======
	status text,
	notes text,
	PRIMARY KEY (cc_no, transaction_id)
) WITH CLUSTERING ORDER BY (transaction_id DESC) 
	 AND compaction= { 'class': 'MemoryOnlyStrategy' }
     AND compression = {'sstable_compression' : ''}
     AND caching = {'keys':'NONE', 'rows_per_partition':'NONE'};

create table if not exists transactions(
	cc_no text,
 	transaction_id timeuuid,
>>>>>>> 59a81d5e776e7631546539816978c1e28024a2e8
	location text,
	items map<text, double>,
	issuer text,
	amount double,
	status text,
	notes text,
	PRIMARY KEY (transaction_id)
<<<<<<< HEAD
) WITH compaction = {'class': 'DateTieredCompactionStrategy'};

create table if not exists blacklist_transactions(
	date text,
	transaction_time timestamp,
 	transaction_id text,
	PRIMARY KEY (date, transaction_time)
) WITH CLUSTERING ORDER BY (transaction_time DESC);
=======
);

create table if not exists blacklist_transactions(
	dummy text,
 	transaction_id timeuuid,
	PRIMARY KEY (dummy)
);
>>>>>>> 59a81d5e776e7631546539816978c1e28024a2e8
     
create table if not exists users(
	user_id text PRIMARY KEY,
	first text,
	last text,
	gender text,
	city text,
	state text,
<<<<<<< HEAD
	cc_no text,
	email text
) WITH compaction= { 'class': 'MemoryOnlyStrategy' }
     AND compression = {'sstable_compression' : ''}
     AND caching = {'keys':'NONE', 'rows_per_partition':'NONE'};
    

//Change some user stuff        
alter table users add email text; 

create table if not exists user_rules (
	userId text,
	ruleId text,
	ruleName text,
	issuer text,
	amount double,
	noOfTransactions int,
	noOfDays int,
	PRIMARY KEY(userId, ruleId)
)WITH compaction= { 'class': 'MemoryOnlyStrategy' }
     AND compression = {'sstable_compression' : ''}
     AND caching = {'keys':'NONE', 'rows_per_partition':'NONE'};
=======
	cc_no text
);
>>>>>>> 59a81d5e776e7631546539816978c1e28024a2e8

create table if not exists issuers(
    id text,
    name text,
    location text,
    PRIMARY KEY (id)
) WITH compaction= { 'class': 'MemoryOnlyStrategy' }
     AND compression = {'sstable_compression' : ''}
     AND caching = {'keys':'NONE', 'rows_per_partition':'NONE'};
    
create table if not exists blacklist_issuers(
	issuer text,
	city text,
	amount double,
	PRIMARY KEY (issuer, city)
) WITH compaction= { 'class': 'MemoryOnlyStrategy' }
     AND compression = {'sstable_compression' : ''}
     AND caching = {'keys':'NONE', 'rows_per_partition':'NONE'};
  	 
	
create table if not exists blacklist_cards(
	dummy text,
	cc_no text,
	amount double,
	PRIMARY KEY (dummy, cc_no)
) WITH compaction= { 'class': 'MemoryOnlyStrategy' }
     AND compression = {'sstable_compression' : ''}
     AND caching = {'keys':'NONE', 'rows_per_partition':'NONE'};
  	 
create table if not exists transaction_date_minute_counter (
	date text,
	hour int, 
	minute int,
	total_for_minute counter,
	PRIMARY KEY  (date, hour, minute)
);

//Issuers
create table if not exists trans_issuer_date_minute_counter (
	issuer text,
	date text,
<<<<<<< HEAD
	hour int,
	minute int,
	total counter,
	PRIMARY KEY  ((issuer, date), hour, minute)	
=======
	hour intm
	minute int,
	total counter,
	PRIMARY KEY  (issuer, date, hour, minute)	
>>>>>>> 59a81d5e776e7631546539816978c1e28024a2e8
);

create table if not exists trans_user_date_minute_counter (
	user text,
	date text,
	hour int,
	minute int,
	total counter,
<<<<<<< HEAD
	PRIMARY KEY  ((user, date), hour, minute)	
=======
	PRIMARY KEY  (user, date, hour, minute)	
>>>>>>> 59a81d5e776e7631546539816978c1e28024a2e8
);

create table if not exists trans_counter (
	date text,
	total counter,
	PRIMARY KEY  (date)
);

<<<<<<< HEAD



truncate trans_issuer_date_minute_counter;
truncate trans_user_date_minute_counter;
truncate transactions;
truncate latest_transactions;
truncate credit_card_transactions_balance;
truncate credit_card_transactions_by_issuer_date;
truncate transaction_date_minute_counter;
truncate trans_counter;
truncate blacklist_transactions;
truncate user_rules;

truncate blacklist_cards;
truncate blacklist_issuers;

truncate users;
truncate issuers;

drop table transactions;
drop table latest_transactions;
drop table credit_card_transactions_balance;
drop table credit_card_transactions_by_issuer_date;
drop table transaction_date_minute_counter;
drop table trans_issuer_date_minute_counter;
drop table trans_user_date_minute_counter;
drop table trans_counter;

drop keyspace datastax_creditcard_demo;
=======
truncate trans_issuer_date_minute_counter;
truncate trans_user_date_minute_counter
truncate blacklist_transactions;
truncate blacklist_cards;
truncate blacklist_issuers;
truncate transactions;
truncate credit_card_transactions_balance;
truncate credit_card_transactions_by_issuer_date;
truncate transaction_date_minute_counter;
truncate users;

drop keyspace datastax_creditcard_demo;

>>>>>>> 59a81d5e776e7631546539816978c1e28024a2e8
