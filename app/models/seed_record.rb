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

    def delete_waste_digests!(seed_table, range_of_ids, master_record_ids)
      seed_record_ids =
        self.by_seed_table_id(seed_table.id).
        by_record_id(range_of_ids).
        pluck(:record_id)

      waste_record_ids = seed_record_ids - master_record_ids
      SeedRecord.by_seed_table_id(seed_table.id).by_record_id(waste_record_ids).delete_all
    end

    def renew_digests!(seed_express, inserted_ids, updated_ids, new_digests)
      @@callbacks = seed_express.callbacks
      pending_records = update_digests!(updated_ids, new_digests)
      inserting_records = make_bulk_digest_records(inserted_ids, new_digests)
      insert_digests!(inserting_records + pending_records)
      delete_digests_out_of!(seed_express.parts)
    end

    private

    def update_digests!(updated_ids, new_digests)
      results = {:inserting_records => []}
      existing_digests = self.all.index_by(&:record_id)
      do_each_block(updated_ids, BLOCK_SIZE,
                    :updating_digests, :updating_a_part_of_digests) do |targets|
        results[:updating_records] = []
        targets.each do |id|
          make_updating_records!(id, existing_digests, new_digests, results)
        end

        bulk_update_digests!(results[:updating_records])
      end

      results[:inserting_records]
    end

    def make_updating_records!(target_id, existing_digests, new_digests, results)
      seed_record = existing_digests[target_id]
      if seed_record
        seed_record.digest = new_digests[target_id]
        results[:updating_records] << seed_record
      else
        results[:inserting_records] <<
          self.new(:record_id => target_id, :digest => new_digests[target_id])
      end
    end

    def make_bulk_digest_records(inserted_ids, new_digests)
      inserting_records = []
      do_each_block(inserted_ids, BLOCK_SIZE,
                    :making_bulk_digest_records, :making_a_part_of_bulk_digest_records) do |targets|
        inserting_records += targets.map do |id|
          self.new(:record_id => id, :digest => new_digests[id])
        end
      end
      inserting_records
    end

    def insert_digests!(bulk_records)
      bulk_records_count = bulk_records.size
      do_each_block(bulk_records, BLOCK_SIZE,
                    :inserting_digests, :inserting_a_part_of_digests) do |targets|
        SeedRecord.import(targets)
      end
    end

    def bulk_update_digests!(records)
      return if records.empty?
      sql = build_update_query(records)
      ActiveRecord::Base.connection.execute(sql)
    end

    def update_query_of_ids(records)
      records.map(&:id).join(',')
    end

    def update_query_of_digests(records)
      records.map { |v| "'#{v.digest}'"  }.join(',')
    end

    def build_update_query(records)
      ids = update_query_of_ids(records)
      digests = update_query_of_digests(records)

      <<-"EOF"
        UPDATE seed_records
        SET
          updated_at = '#{Time.zone.now.utc.iso8601}',
          digest = ELT(FIELD(id, #{ids}), #{digests})
        WHERE
          id IN (#{ids})
      EOF
    end

    def delete_digests_out_of!(parts)
      self.by_out_of_parts(parts).delete_all
    end

    def callbacks
      @@callbacks
    end
  end
end
