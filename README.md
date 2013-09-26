CQL Reference:

http://www.datastax.com/docs/1.1/references/cql/index

Handy Cassandra Commands
Show Keyspaces

echo 'show keyspaces;' | cassandra-cli |grep ^Keyspace




Get your Datastax credentials from http://www.datastax.com/download/enterprise
and create a file in your root directory called datastax_credentials.yml.

How to start DSE:
http://www.datastax.com/docs/datastax_enterprise3.0/reference/start_stop_dse#start-dse-service

Import data via JDBC:
http://www.datastax.com/docs/datastax_enterprise3.0/solutions/sqoop_cf_migration#sqoop-cf-migrate


Currently following this one:
http://kkpradeeban.blogspot.ca/2012/06/moving-data-from-mysql-to-cassandra.html


sudo dse sqoop import --connect jdbc:mysql://carls-macbook-pro.local/lp_webapp \
  --username root \
  --table users \
  --cassandra-keyspace lp_webapp \
  --cassandra-column-family users \
  --cassandra-row-key id \
  --cassandra-thrift-host 127.0.0.1 \
  --cassandra-create-schema

sudo dse sqoop import --connect jdbc:mysql://carls-macbook-pro.local/lp_webapp \
  --username root \
  --table pages \
  --cassandra-keyspace lp_webapp \
  --cassandra-column-family pages \
  --cassandra-row-key uuid \
  --cassandra-thrift-host 127.0.0.1


CREATE TABLE pages (
  id bigint,
  uuid uuid,
  url varchar,
  created_at timestamp,
  updated_at timestamp,
  company_id bigint,
  name varchar,
  state varchar,
  last_published_at timestamp,
  champion_variant_id varchar,
  cta varchar,
  description varchar,
  deleted_at timestamp,
  type varchar,
  page_variant_base_id bigint,
  used_as varchar,
  path_name varchar,
  modified_at timestamp,
  shared boolean,
  template_order int,
  variants_count int,
  testing_variants_count int,
  integration_errors_count bigint,
  processing boolean,
  PRIMARY KEY (uuid)
);

CREATE INDEX by_company_id on pages(company_id),
#KEY url (url),
#KEY page_variant_base_id (page_variant_base_id)


CREATE TABLE form_submissions (
  id bigint PRIMARY KEY,
  page_uuid varchar,
  form_data varchar,
  submitter_ip varchar,
  variant_id varchar,
  created_at timestamp,
  updated_at timestamp,
  state varchar,
  extra_data varchar
);


CREATE INDEX by_page_uuid on form_submissions(page_uuid);


sudo dse sqoop import --connect jdbc:mysql://carls-macbook-pro.local/lp_webapp \
  --username root \
  --table form_submissions \
  --cassandra-keyspace lp_webapp \
  --cassandra-column-family form_submissions \
  --cassandra-row-key id \
  --cassandra-thrift-host 127.0.0.1
