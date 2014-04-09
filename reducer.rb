require "rubygems"
require "aws-sdk"
require "zlib"
require "stringio"
require "active_record"
require "mysql2"
require "riak"

RIAK_BUCKETS = {
  "ticket_body" => "t_b",
  "note_body" => "n_b"
}



$node_client = Riak::Client.new(:nodes => [
                                  {:host => '10.0.0.2', :pb_port => 1234},
                                  {:host => '10.0.0.3', :pb_port => 5678}
])

$ticket_body = $node_client.bucket(RIAK_BUCKETS["ticket_body"])
$note_body = $node_client.bucket(RIAK_BUCKETS["note_body"])

ActiveRecord::Base.configurations = {
  'shard_1' => {
    'adapter'  => 'mysql2',
    'host'     => 'localhost',
    'username' => 'root',
    'password' => 'root',
    'database' => 'helpkit16'
  },
  'shard_2' => {
    'adapter'  => 'mysql2',
    'host'     => 'localhost',
    'username' => 'root',
    'password' => 'root',
    'database' => 'helpkit15'
  }
}

TicketEndingId = {
  "shard_1" => "10",
  "shard_2" => "10"
}

NoteEndingId = {
  "shard_1" => "10",
  "shard_2" => "10"
}

BatchSize = 1000
TicketBodyBucket = "ticketbody-dev"
NoteBodyBucket = "notebody-dev"

ConnectionPool = {}

# ActiveRecord::Base.configurations.each do |key,value|
# 	ConnectionPool[key] = ActiveRecord::Base.establish_connection key
# end

class Account < ActiveRecord::Base
end

class HelpdeskTicket < ActiveRecord::Base
end

class HelpdeskNote < ActiveRecord::Base
end

def s3_partition(key_id)
  key_id.to_s.reverse
end

def generate_key(account_id,key_id)
  s3_partition(key_id) + "/#{account_id}/#{key_id}"
end

def generate_file_path_ticket(account_id,ticket_id)
  generate_key(account_id,ticket_id) + "/ticket_body.json"
end

def generate_file_path_note(account_id,ticket_id)
  generate_key(account_id,ticket_id) + "/ticket_body.json"
end

def read(key, bucket_name)
  json_data = AWS::S3::Bucket.new(bucket_name).objects[key].read(:content_type => 'application/json')
end

def store_in_riak_ticket(key,value)
  obj = $ticket_body.get_or_new(key)
  unless obj.data
    obj.content_type = "text/plain"
    obj.data = compress(value)
    obj.store
  end
end

def store_in_riak_note(key,value)
  obj = $note_body.get_or_new(key)
  unless obj.data
    obj.content_type = "text/plain"
    obj.data = compress(value)
    obj.store
  end
end

def compress(text)
  data = StringIO.new ""
  gzip_writer = Zlib::GzipWriter.new data
  gzip_writer.write(text)
  gzip_writer.finish;
  data.string
end

def migrate_tickets_to_riak(account_id,ending_id)
  HelpdeskTicket.find_in_batches(:batch_size => BatchSize,
  :conditions => ["account_id =? and id <= ?",account_id,ending_id], :select => "id") do |helpdesk_tickets|
    helpdesk_tickets.each do |ticket|
      s3_key = generate_file_path_ticket(account_id,ticket.id)
      riak_key = "#{account_id}/#{ticket.id}"
      value = read(s3_key, TicketBodyBucket)
      store_in_riak_ticket(riak_key,value)
    end
  end
end

def migrate_notes_to_riak(account_id,ending_id)
  HelpdeskNote.find_in_batches(:batch_size => BatchSize,
  :conditions => ["account_id =? and id <= ?",account_id,ending_id], :select => "id") do |helpdesk_notes|
    helpdesk_notes.each do |note|
      s3_key = generate_file_path_note(account_id,note.id)
      riak_key = "#{account_id}/#{note.id}"
      value = read(s3_key, NoteBodyBucket)
      store_in_riak_ticket(riak_key,value)
    end
  end
end

ARGF.each do |line|
  # remove any newline
  line = line.chomp

  # split key and value on tab character
  (account_id, shard) = line.split(/\t/)
  ActiveRecord::Base.establish_connection shard
  ending_id_tickets = TicketEndingId[shard]
  ending_id_notes = NoteEndingId[shard]
  migrate_tickets_to_riak(account_id,ending_id_tickets)
  migrate_notes_to_riak(account_id,ending_id_notes)
end
