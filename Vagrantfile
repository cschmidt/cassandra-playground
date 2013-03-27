# -*- mode: ruby -*-
# vi: set ft=ruby :

require 'yaml'

Vagrant::Config.run do |config|
  config.vm.define :cassandra_node do |node|
    node.vm.box = 'quantal64'
    node.vm.box_url = 'http://cloud-images.ubuntu.com/quantal/current/quantal-server-cloudimg-vagrant-amd64-disk1.box'
    node.vm.host_name = 'cassandra01'
    # Having enough memory around for Cassandra is pretty important.  I ran into
    # an issue where the Cassandra server process kept getting killed by the
    # kernel... java.net.ConnectException: Connection refused
    node.vm.customize ['modifyvm', :id, "--memory", '2048']
    node.vm.network :hostonly, '10.63.1.50'
    # share a tmp folder so that we don't have to re-download large binaries
    %x[mkdir -p /tmp/vagrant/downloads]
    node.vm.share_folder "tmp-downloads", '/tmp/downloads', '/tmp/vagrant/downloads'

    node.vm.provision :chef_solo do |chef|
      chef.cookbooks_path = 'cookbooks'
      chef.add_recipe 'apt'
      chef.add_recipe 'cassandra'
      datastax_credentials = YAML::load(File.open('datastax_credentials.yml'))
      chef.json = datastax_credentials
    end
  end
end
