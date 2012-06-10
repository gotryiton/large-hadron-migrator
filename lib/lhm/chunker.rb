# Copyright (c) 2011, SoundCloud Ltd., Rany Keddo, Tobias Bielohlawek, Tobias
# Schmidt

require 'lhm/command'
require 'lhm/sql_helper'

module Lhm
  class Chunker
    include Command
    include SqlHelper

    attr_reader :connection

    # Copy from origin to destination in chunks of size `stride`. Sleeps for
    # `throttle` milliseconds between each stride.
    def initialize(migration, connection = nil, options = {})
      @migration = migration
      @connection = connection
      @stride = options[:stride] || 40_000
      @throttle = options[:throttle] || 100
      @start = options[:start] || select_start
      @limit = options[:limit] || select_limit
    end

    # Copies chunks of size `stride`, starting from `start` up to id `limit`.
    def up_to(&block)
      1.upto(traversable_chunks_size) do |n|
        yield(bottom(n), top(n))
      end
    end

    def traversable_chunks_size
      @limit && @start ? ((@limit - @start + 1) / @stride.to_f).ceil : 0
    end

    def bottom(chunk)
      (chunk - 1) * @stride + @start
    end

    def top(chunk)
      [chunk * @stride + @start - 1, @limit].min
    end

    def copy(lowest, highest)
      "insert ignore into `#{ destination_name }` (#{ columns }) " +
      "select #{ columns_with_joins } from `#{ origin_name }` " +
      "#{ joins } " +
      "where #{origin_name}.`id` between #{ lowest } and #{ highest }"
    end

    def joins
      @migration.insert_joins.map{|j| "join #{ j[:table] } on #{ j[:statement] }"}.join(" ")
    end

    def join_fields_typed
      @migration.insert_joins.map {|j| "#{j[:table]}.`#{j[:origin_field]}`"}
    end

    def join_fields
      @migration.insert_joins.map {|j| "`#{ j[:destination_field] }`"}
    end

    def select_start
      start = connection.select_value("select min(id) from #{ origin_name }")
      start ? start.to_i : nil
    end

    def select_limit
      limit = connection.select_value("select max(id) from #{ origin_name }")
      limit ? limit.to_i : nil
    end

    def throttle_seconds
      @throttle / 1000.0
    end

  private

    def destination_name
      @migration.destination.name
    end

    def origin_name
      @migration.origin.name
    end

    def columns
      @columns ||= (@migration.intersection.escaped + join_fields).join(", ")
    end

    def columns_with_joins
      @columns_with_joins ||= (@migration.intersection.typed_unjoined(origin_name) + join_fields_typed).join(", ")
    end

    def validate
      if @start && @limit && @start > @limit
        error("impossible chunk options (limit must be greater than start)")
      end
    end

    def execute
      up_to do |lowest, highest|
        affected_rows = update(copy(lowest, highest))

        if affected_rows > 0
          sleep(throttle_seconds)
        end

        print "."
      end
      print "\n"
    end
  end
end
