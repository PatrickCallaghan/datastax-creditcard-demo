create keyspace if not exists datastax_creditcard_demo WITH replication = {'class': 'NetworkTopologyStrategy', 'Analytics': 1};

use datastax_creditcard_demo; 
 
CREATE TABLE if not exists credit_card_transactions_balance (
	cc_no text,
	transaction_time timestamp,
	transaction_id text,
	amount double,
	balance_at timestamp static,
	balance double static,
	PRIMARY KEY (cc_no, transaction_time )
) WITH CLUSTERING ORDER BY (transaction_time DESC);

CREATE TABLE if not exists credit_card_transactions_by_issuer_date (
	issuer text,
	transaction_time timestamp,
	date text, 
	transaction_id text,
	cc_no text,
	amount double,
 	PRIMARY KEY ((issuer, date), transaction_time)
) WITH CLUSTERING ORDER BY (transaction_time DESC);
	
create table if not exists latest_transactions(
	cc_no text,
	transaction_time timestamp,
	transaction_id text,
	location text,
	items map<text, double>,
	issuer text,
	amount double,
	status text,
	notes text,
	PRIMARY KEY (cc_no, transaction_id)
) WITH compaction= { 'class': 'MemoryOnlyStrategy' }
     AND compression = {'sstable_compression' : ''}
     AND caching = {'keys':'NONE', 'rows_per_partition':'NONE'};

create table if not exists transactions(
	cc_no text,
	transaction_time timestamp,
 	transaction_id text,
	location text,
	items map<text, double>,
	issuer text,
	amount double,
	status text,
	notes text,
	PRIMARY KEY (transaction_id)
);

create table if not exists blacklist_transactions(
	date text,
	transaction_time timestamp,
 	transaction_id text,
	PRIMARY KEY (date, timestamp)
) WITH CLUSTERING ORDER BY (transaction_time DESC);
     
create table if not exists users(
	user_id text PRIMARY KEY,
	first text,
	last text,
	gender text,
	city text,
	state text,
	cc_no text
) WITH compaction= { 'class': 'MemoryOnlyStrategy' }
     AND compression = {'sstable_compression' : ''}
     AND caching = {'keys':'NONE', 'rows_per_partition':'NONE'};

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
	hour int,
	minute int,
	total counter,
	PRIMARY KEY  ((issuer, date), hour, minute)	
);

create table if not exists trans_user_date_minute_counter (
	user text,
	date text,
	hour int,
	minute int,
	total counter,
	PRIMARY KEY  ((user, date), hour, minute)	
);

create table if not exists trans_counter (
	date text,
	total counter,
	PRIMARY KEY  (date)
);




truncate trans_issuer_date_minute_counter;
truncate trans_user_date_minute_counter;
truncate transactions;
truncate latest_transactions;
truncate credit_card_transactions_balance;
truncate credit_card_transactions_by_issuer_date;
truncate transaction_date_minute_counter;
truncate trans_counter;

truncate blacklist_transactions;
truncate blacklist_cards;
truncate blacklist_issuers;

truncate users;
truncate issuers;

drop keyspace datastax_creditcard_demo;


drop table transactions;
drop table latest_transactions;
drop table credit_card_transactions_balance;
drop table credit_card_transactions_by_issuer_date;
drop table transaction_date_minute_counter;
drop table trans_issuer_date_minute_counter;
drop table trans_user_date_minute_counter;
drop table trans_counter;


