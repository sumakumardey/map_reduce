require "rubygems"
require "aws-sdk"
require "zlib"
require "stringio"
require "active_record"
require "mysql2"

DatabaseSetting = {
  :global => {
    :adapter => "mysql2",
    :host => "localhost",
    :database => "helpkit16",
    :username => "root",
    :password => "root",
    :port => "3306"
  }
}

ActiveRecord::Base.establish_connection(
  DatabaseSetting[:global]
)

class ShardMapping < ActiveRecord::Base
end

def map
  ShardMapping.all.each do |shard_mapping|
    puts shard_mapping.account_id.to_s + "\t" + shard_mapping.shard_name
  end
end

map
