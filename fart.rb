#!/usr/bin/env ruby

require 'aws-sdk'
require 'json'
require 'sqlite3'

s3_paths = [
  { bucket: 'awaxa-dcim', prefix: 'mov/' },
  { bucket: 'awaxa-dcim', prefix: 'nef/' },
  { bucket: 'awaxa-mp3'},
  { bucket: 'awaxa-archive', prefix: 'archive/' },
]

db = SQLite3::Database.new "objects.db"
db.results_as_hash = true
db.execute <<-SQL
create table if not exists objects (
    id integer primary key autoincrement,
    bucket varchar(63) not null,
    key varchar(1024) not null,
    size integer not null,
    batch integer,
    confirmed bool
  );
SQL
records = db.execute('select count(id) from objects').first['count(id)']

if records == 0
  puts "objects.db empty, querying s3"
  s3 = Aws::S3::Client.new(region: 'us-east-1')
  s3_paths.each do |h|
    print "#{h[:bucket]}/#{h[:prefix]} " # illustrate pogress
    s3.list_objects({
      bucket: h[:bucket],
      prefix: h[:prefix],
    }).each do |resp| # handle >1000 objects
      resp.contents.each do |obj|

        bucket = h[:bucket]
        key = obj.key
        size = obj.size

        # skip bucket+prefix non_object in response
        next if size == 0 and key[-1] == '/'

        db.execute("insert into objects (bucket, key, size) values (?, ?, ?)",
                   [bucket, key, size])

        print '.' # illustrate progress
      end
    end
    printf "\n" # illustrate progress
  end
end

total = db.execute('select sum(size) from objects').first['sum(size)']
biggest = db.execute('select * from objects order by size desc limit 1').first
batch_size_max = biggest['size']
num_batches = 3/2 * total/batch_size_max

current_batch = 0
db.execute('select * from objects where batch is null order by size desc').each do |row|
  db.execute("update objects set batch=#{current_batch} where id=#{row['id']}")
  print '.' # illustrate progress
  if current_batch == num_batches
    current_batch = 1
  else
    current_batch = current_batch + 1
  end
end
printf "\n" # illustrate progress

# require 'pry'; binding.pry ; exit 123

(0..num_batches).each do |batch|
  total = db.execute("select sum(size) from objects where batch=#{batch}").first['sum(size)']
  puts "batch #{batch} total: #{total}"
end
