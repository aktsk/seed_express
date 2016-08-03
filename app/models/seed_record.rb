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
    def renew_digests!(seed_express, inserted_ids, updated_ids, new_digests)
      pending_records = update_digests!(seed_express, updated_ids, new_digests)
      inserting_records = make_bulk_digest_records(seed_express, inserted_ids, new_digests)
      insert_digests!(seed_express, inserting_records + pending_records)
      delete_digests_out_of!(seed_express.parts)
    end

    private

    def update_digests!(seed_express, updated_ids, new_digests)
      inserting_records = []
      existing_digests = self.all.index_by(&:record_id)
      counter = 0
      seed_express.callbacks[:before_updating_digests].call(counter, updated_ids.size)
      updated_ids.each_slice(BLOCK_SIZE) do |part_of_updated_ids|
        seed_express.callbacks[:before_updating_a_part_of_digests].call(counter, updated_ids.size)
        updating_records = []
        part_of_updated_ids.each do |id|
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
        counter += part_of_updated_ids.size
        seed_express.callbacks[:after_updating_a_part_of_digests].call(counter, updated_ids.size)
      end

      seed_express.callbacks[:after_updating_digests].call(counter, updated_ids.size)
      inserting_records
    end

    def make_bulk_digest_records(seed_express, inserted_ids, new_digests)
      counter = 0
      inserting_records = []
      seed_express.callbacks[:before_making_bulk_digest_records].call(0, inserted_ids.size)
      inserted_ids.each do |id|
        if counter % BLOCK_SIZE == 0
          seed_express.callbacks[:before_making_a_part_of_bulk_digest_records].call(counter, inserted_ids.size)
        end

        inserting_records <<
          self.new(:record_id => id, :digest => new_digests[id])

        counter += 1
        if counter % BLOCK_SIZE == 0
          seed_express.callbacks[:after_making_a_part_of_bulk_digest_records].call(counter, inserted_ids.size)
        end
      end

      seed_express.callbacks[:after_making_bulk_digest_records].call(inserted_ids.size, inserted_ids.size)
      inserting_records
    end

    def insert_digests!(seed_express, bulk_records)
      counter = 0
      bulk_records_count = bulk_records.size
      seed_express.callbacks[:before_inserting_digests].call(counter, bulk_records_count)
      while bulk_records.present?
        seed_express.callbacks[:before_inserting_a_part_of_digests].call(counter, bulk_records_count)
        targets = bulk_records.slice!(0, BLOCK_SIZE)
        SeedRecord.import(targets)
        counter += targets.size
        seed_express.callbacks[:after_inserting_a_part_of_digests].call(counter, bulk_records_count)
      end
      seed_express.callbacks[:after_inserting_digests].call(counter, bulk_records_count)
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
  end
end
