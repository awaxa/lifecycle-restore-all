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
records = db.execute('select count(id) from objects')[0]['count(id)']

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

def describe(row)
  "#{row['size']} #{row['bucket']}/#{row['key']}"
end

def debug(msg)
  # if debugging
  puts msg
end

biggest = db.execute('select * from objects order by size desc limit 1').first
batch_size_max = biggest['size']
db.execute("update objects set batch=1 where id=#{biggest['id']}")
debug("batch 1 << #{describe(biggest)}")

while db.execute('select count(id) from objects where batch is null')[0]['count(id)'] > 0
  batch = db.execute('select max(batch) from objects')[0]['max(batch)'] + 1
  batch_size_current = 0
  while batch_size_max >= batch_size_current
    max_object_size = batch_size_max - batch_size_current
    object_arr = db.execute("select * from objects where batch is null and size < #{max_object_size} limit 1")
    break if object_arr.length == 0 # end batch if nothing else fits
    obj = object_arr.first

    db.execute("update objects set batch=#{batch} where id=#{obj['id']}")
    batch_size_current = batch_size_current + obj['size']
    debug("batch #{batch} << #{describe(obj)}")
  end
  debug("\n\nbatch #{batch} complete, total #{batch_size_current}, remaining #{batch_size_max - batch_size_current}\n\n")
end

batches = db.execute('select distinct batch from objects where batch not null order by batch')
debug("\nbatches #{batches}")
require 'pry'; binding.pry
