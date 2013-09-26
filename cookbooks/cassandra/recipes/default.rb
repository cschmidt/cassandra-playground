DOWNLOAD_DIR = '/tmp/downloads'
MYSQL_CONNECTOR_DOWNLOAD_URL = 'http://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-java-5.1.26.tar.gz/from/http://cdn.mysql.com/'
MYSQL_CONNECTOR_DOWNLOAD_FILE = "#{DOWNLOAD_DIR}/mysql-connector-java-5.1.26.tar.gz"

DEB_USER = node['datastax_debian_repo']['user']
DEB_PW = node['datastax_debian_repo']['password']

directory DOWNLOAD_DIR

apt_repository 'apache-cassandra' do
  uri 'http://www.apache.org/dist/cassandra/debian'
  distribution '12x'
  components ['main']
  keyserver 'pool.sks-keyservers.net'
  key '4BD736A82B5C1B00'
end

apt_repository 'datastax-cassandra' do
  uri "http://#{DEB_USER}:#{DEB_PW}@debian.datastax.com/enterprise"
  components ['main']
  distribution 'stable'
  key 'http://debian.datastax.com/debian/repo_key'
end

script 'download JDK 6' do
  interpreter 'bash'
  user 'root'
  not_if {File.exists? '/vagrant/jdk-6u43-linux-x64.bin'}
  code <<-SCRIPT
    cd /vagrant
    wget --continue --no-cookies --header "Cookie: gpw_e24=http%3A%2F%2Fwww.oracle.com" "http://download.oracle.com/otn-pub/java/jdk/6u43-b01/jdk-6u43-linux-x64.bin"
  SCRIPT
end

script 'install JDK 6' do
  interpreter 'bash'
  user 'root'
  not_if {File.exists? '/usr/java/latest/jdk1.6.0_43'}
  code <<-SCRIPT
    mkdir -p /usr/java/latest
    cp /vagrant/jdk-6u43-linux-x64.bin /usr/java/latest
    cd /usr/java/latest
    chmod a+x jdk-6u43-linux-x64.bin
    ./jdk-6u43-linux-x64.bin
    update-alternatives --install "/usr/bin/java" "java" "/usr/java/latest/jdk1.6.0_43/bin/java" 1
    update-alternatives --set java /usr/java/latest/jdk1.6.0_43/bin/java
  SCRIPT
end

# For Apache Cassandra
# package 'cassandra'

# For Datastax DSE

# libssl required by opscenter (but not specified as a req)
package 'libssl0.9.8'
package 'dse-full'
package 'opscenter'

# FIXME: this should move to a more general recipe, this is just to allow
# local name resolution.  Nothing to do with Cassandra per se.
package 'avahi-daemon'

remote_file MYSQL_CONNECTOR_DOWNLOAD_FILE do
  source MYSQL_CONNECTOR_DOWNLOAD_URL
  mode 0644
  action :create_if_missing
  checksum 'cba8fbf00025e5f24086840c8bd60547'
end

service 'dse' do
  action [:start, :enable]
end
