create keyspace if not exists datastax_creditcard_demo WITH replication = {'class': 'NetworkTopologyStrategy', 'Analytics': 1};

use datastax_creditcard_demo; 
 
CREATE TABLE if not exists credit_card_transactions_balance (
	cc_no text,
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
	location text,
	items map<text, double>,
	issuer text,
	amount double,
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
	location text,
	items map<text, double>,
	issuer text,
	amount double,
	status text,
	notes text,
	PRIMARY KEY (transaction_id)
);

create table if not exists blacklist_transactions(
	dummy text,
 	transaction_id timeuuid,
	PRIMARY KEY (dummy)
);
     
create table if not exists users(
	user_id text PRIMARY KEY,
	first text,
	last text,
	gender text,
	city text,
	state text,
	cc_no text
);

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
	hour intm
	minute int,
	total counter,
	PRIMARY KEY  (issuer, date, hour, minute)	
);

create table if not exists trans_user_date_minute_counter (
	user text,
	date text,
	hour int,
	minute int,
	total counter,
	PRIMARY KEY  (user, date, hour, minute)	
);

create table if not exists trans_counter (
	date text,
	total counter,
	PRIMARY KEY  (date)
);

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

