# -*- mode: ruby -*-
# vi: set ft=ruby :

require 'yaml'

Vagrant.configure("2") do |config|
  config.vm.define :cassandra_node do |node|
    node.vm.box = 'quantal64'
    node.vm.box_url = 'http://cloud-images.ubuntu.com/quantal/current/quantal-server-cloudimg-vagrant-amd64-disk1.box'
    node.vm.hostname = 'cassandra01'
    node.vm.network :private_network, ip: '10.63.1.50'
    # share a tmp folder so that we don't have to re-download large binaries
    %x[mkdir -p /tmp/vagrant/downloads]
    node.vm.synced_folder '/tmp/vagrant/downloads', '/tmp/downloads'

    node.vm.provision :chef_solo do |chef|
      chef.cookbooks_path = 'cookbooks'
      chef.add_recipe 'apt'
      chef.add_recipe 'cassandra'
      datastax_credentials = YAML::load(File.open('datastax_credentials.yml'))
      chef.json = datastax_credentials
    end

    node.vm.provider :virtualbox do |vb|
      # Having enough memory around for Cassandra is pretty important.  I ran into
      # an issue where the Cassandra server process kept getting killed by the
      # kernel... java.net.ConnectException: Connection refused
      vb.customize ['modifyvm', :id, "--memory", '2048']      
    end

    node.vm.provider :aws do |aws|
      aws.access_key_id = "YOUR KEY"
      aws.secret_access_key = "YOUR SECRET KEY"
      aws.keypair_name = "KEYPAIR NAME"
      aws.ssh_private_key_path = "PATH TO YOUR PRIVATE KEY"

      aws.ami = "ami-7747d01e"
      aws.ssh_username = "ubuntu"
    end

  end

end
