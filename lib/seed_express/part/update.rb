# -*- coding: utf-8 -*-
module SeedExpress
  class Part
    module Update
      def update_records(records)
        results = {:updated_ids => [], :actual_updated_ids => []}
        do_each_block(records, BLOCK_SIZE,
                      :updating, :updating_a_part) do |targets|
          mix_results!(results, update_a_block_of_records(targets))
        end
        results
      end

      def update_a_block_of_records(records)
        existing_records = existing_records_by_id(records)
        results = {:updated_ids => [], :actual_updated_ids =>[], :error => false}
        ActiveRecord::Base.transaction do
          records.each do |record|
            update_a_record!(record, existing_records, results)
          end
        end

        results
      end

      def existing_records_by_id(records_from_file)
        record_ids = records_from_file.map { |target| target[:id] }
        target_model.unscoped.where(:id => record_ids).index_by(&:id)
      end

      def update_a_record!(record, existing_records, results)
        id = record[:id]
        model = existing_records[id]
        set_value_into_model!(record, model)
        if model.changed?
          if model.valid?
            model.save!
            results[:actual_updated_ids] << id
          else
            show_each_validation_error(model)
            results[:error] = true
          end
        end
        results[:updated_ids] << id
      end
    end
  end
end
