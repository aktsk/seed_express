# -*- coding: utf-8 -*-
module SeedExpress
  class Part
    module Insert
      def insert_records(records)
        results = {:inserted_ids => []}
        do_each_block(records, BLOCK_SIZE, :inserting, :inserting_a_part) do |targets|
          mix_results!(results, insert_a_block_of_records(targets))
        end
        results
      end

      def insert_a_block_of_records(records)
        error = false
        inserted_ids = []
        bulk_models = records.map do |record|
          model = target_model.new
          set_value_into_model!(record, model)
          if model.valid?
            inserted_ids << record[:id]
            model
          else
            show_each_validation_error(model)
            error = true
            nil
          end
        end.compact
        target_model.import(bulk_models)
        error |= detect_an_error_of_bulk_import(inserted_ids)

        return :inserted_ids => inserted_ids, :error => error
      end

      def detect_an_error_of_bulk_import(inserted_ids)
        actual_inserted_ids = ActiveRecord::Base.transaction do
          target_model.unscoped.where(:id => inserted_ids).pluck(:id)
        end

        lacking_ids = inserted_ids - actual_inserted_ids
        return false if lacking_ids.blank?

        ids_string = lacking_ids.join(', ')
        STDOUT.puts
        STDOUT.puts "Inserting error has been detected caused by ID: #{ids_string}. Maybe it's duplicated keys on not ID column. Fix it, and then run with truncate mode."

        true
      end
    end
  end
end
