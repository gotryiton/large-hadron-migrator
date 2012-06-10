# Copyright (c) 2011, SoundCloud Ltd., Rany Keddo, Tobias Bielohlawek, Tobias
# Schmidt

module Lhm
  #  Determine and format columns common to origin and destination.
  class Intersection
    def initialize(origin, destination, insert_trigger_additions)
      @origin = origin
      @destination = destination
      @insert_trigger_additions = insert_trigger_additions
    end

    def common
      (@origin.columns.keys & @destination.columns.keys).sort
    end

    def insert
      @insert_trigger_additions.keys
    end

    def escaped_insert
      insert.map { |name| tick(name)  }
    end

    def combined_joined
      (escaped + escaped_insert).join(", ")
    end

    def combined_typed(type)
      (common.map { |name| qualified(name, type)  } + @insert_trigger_additions.values.map { |name| parenthesize(name) }).join(", ")
    end

    def escaped
      common.map { |name| tick(name)  }
    end

    def joined
      escaped.join(", ")
    end

    def typed_unjoined(type)
      common.map { |name| qualified(name, type)  }
    end

    def typed(type)
      common.map { |name| qualified(name, type)  }.join(", ")
    end

  private

    def qualified(name, type)
      "#{ type }.`#{ name }`"
    end

    def tick(name)
      "`#{ name }`"
    end

    def parenthesize(name)
      "(#{ name })"
    end
  end
end
