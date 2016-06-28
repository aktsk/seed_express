# -*- coding: utf-8 -*-
module SeedExpress
  class Parts
    include Enumerable
    extend Memoist

    def initialize(seed_table, files, parts)
      parts_keys = parts.keys
      files_keys = files.keys

      obsolete_keys = parts_keys - files_keys
      new_keys = files_keys - parts_keys
      existing_keys = files_keys & parts_keys

      @obsolete_parts = part_objects(seed_table, obsolete_keys, files, parts)
      alive_keys = existing_keys + new_keys
      @alive_parts = part_objects(seed_table, alive_keys, files, parts)
    end

    def each(&block)
      block ? @alive_parts.each(&block) : Enumerator.new(@alive_parts, :each)
    end

    def renew_digests!
      SeedPart.where(:id => @obsolete_parts.map(&:id)).delete_all
      self.each { |part| part.update_digest! }
    end

    def count
      self.each.count
    end

    def size
      self.count
    end
    memoize :size

    private

    def part_objects(seed_table, keys, files, parts)
      keys.map do |key|
        SeedPart.setup(seed_table, key, files[key], parts[key])
      end
    end
  end
end
