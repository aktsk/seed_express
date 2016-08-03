class SeedRecord < ActiveRecord::Base
  belongs_to :seed_table
  scope :by_seed_table_id, ->(seed_table_id) { where(:seed_table_id => seed_table_id) }
  scope :by_record_id, ->(record_id) { where(:record_id => record_id) }
  scope :by_out_of_parts, ->(parts) {
    in_range = parts.map do |part|
      "(record_id BETWEEN #{part.record_id_from} AND #{part.record_id_to})"
    end.join(' OR ')
    where("NOT (#{in_range})")
  }

  BLOCK_SIZE = 5000
  class << self
    include SeedExpress::Utilities

    def renew_digests!(seed_express, inserted_ids, updated_ids, new_digests)
      @@callbacks = seed_express.callbacks
      pending_records = update_digests!(updated_ids, new_digests)
      inserting_records = make_bulk_digest_records(inserted_ids, new_digests)
      insert_digests!(inserting_records + pending_records)
      delete_digests_out_of!(seed_express.parts)
    end

    private

    def update_digests!(updated_ids, new_digests)
      inserting_records = []
      existing_digests = self.all.index_by(&:record_id)
      callbacks[:before_updating_digests].call(0, updated_ids.size)
      do_each_block!(updated_ids, BLOCK_SIZE, :updating_a_part_of_digests) do |targets|
        updating_records = []
        targets.each do |id|
          seed_record = existing_digests[id]
          if seed_record
            seed_record.digest = new_digests[id]
            updating_records << seed_record
          else
            inserting_records <<
              self.new(:record_id => id, :digest => new_digests[id])
          end
        end

        bulk_update_digests!(updating_records)
      end

      callbacks[:after_updating_digests].call(updated_ids.size, updated_ids.size)
      inserting_records
    end

    def make_bulk_digest_records(inserted_ids, new_digests)
      inserting_records = []
      callbacks[:before_making_bulk_digest_records].call(0, inserted_ids.size)
      do_each_block!(inserted_ids, BLOCK_SIZE, :making_a_part_of_bulk_digest_records) do |targets|
        inserting_records += targets.map do |id|
          self.new(:record_id => id, :digest => new_digests[id])
        end
      end
      callbacks[:after_making_bulk_digest_records].call(inserted_ids.size, inserted_ids.size)
      inserting_records
    end

    def insert_digests!(bulk_records)
      bulk_records_count = bulk_records.size
      callbacks[:before_inserting_digests].call(0, bulk_records.size)
      do_each_block!(bulk_records, BLOCK_SIZE, :inserting_a_part_of_digests) do |targets|
        SeedRecord.import(targets)
      end
      callbacks[:after_inserting_digests].call(bulk_records.size, bulk_records.size)
    end

    def bulk_update_digests!(records)
      return if records.empty?

      ids = records.map(&:id).join(',')
      digests = records.map { |v| "'#{v.digest}'"  }.join(',')
      updated_at = "'" + Time.zone.now.utc.strftime('%Y-%m-%dT%H:%M:%S') + "'"

      sql = <<-"EOF"
        UPDATE seed_records
        SET
          updated_at = #{updated_at},
          digest = ELT(FIELD(id, #{ids}), #{digests})
        WHERE
          id IN (#{ids})
      EOF

      ActiveRecord::Base.connection.execute(sql)
    end

    def delete_digests_out_of!(parts)
      self.by_out_of_parts(parts).delete_all
    end

    def callbacks
      @@callbacks
    end
  end
end
