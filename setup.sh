#!/bin/bash

echo 'moving forward in time to a year starting with 2'
sudo apt-get update
sudo apt-get -y upgrade
echo 'unsuckifying system by installing basic libs that should have been there in the first place'
sudo apt-get install -y build-essential openssl libreadline6 libreadline6-dev curl git-core zlib1g zlib1g-dev libssl-dev libyaml-dev libxml2-dev libxslt-dev
echo 'done'
echo 'installing ruby and rubygems'
sudo apt-get -y -t universe install ruby rubygems
echo 'done'
echo 'installing AWS Ruby SDK which should also have been there in the first place'
sudo gem install aws-sdk --source http://rubygems.org
echo 'installing active record'
sudo gem install activerecord -v 2.3.18 --source http://rubygems.org
echo 'installing mysql2 for accessing mysql2'
sudo gem install mysql2 -v 2.3.18 --source http://rubygems.org
echo 'installing riak-client for accessing riak'
sudo gem install riak-client -v 1.4.2 --source http://rubygems.org

echo 'done'
echo 'OK, good to go'
exit 0
