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
db.execute <<-SQL
create table if not exists objects (
    id integer primary key autoincrement,
    bucket varchar(63) not null,
    key varchar(1024) not null,
    size integer not null
  );
SQL
records = db.execute('select count(*) from objects;').flatten.first

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
    printf "\n" # illustrate pogress
  end
end


