# Copyright (c) 2011, SoundCloud Ltd., Rany Keddo, Tobias Bielohlawek, Tobias
# Schmidt

require 'lhm/intersection'

module Lhm
  class Migration
    attr_reader :origin, :destination, :insert_joins

    def initialize(origin, destination, time = Time.now, insert_trigger_additions, insert_joins)
      @origin = origin
      @destination = destination
      @start = time
      @insert_trigger_additions = insert_trigger_additions
      @insert_joins = insert_joins
    end

    def archive_name
      "lhma_#{ startstamp }_#{ @origin.name }"
    end

    def intersection
      Intersection.new(@origin, @destination, @insert_trigger_additions)
    end

    def startstamp
      @start.strftime "%Y_%m_%d_%H_%M_%S_#{ "%03d" % (@start.usec / 1000) }"
    end
  end
end
