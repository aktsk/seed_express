# -*- coding: utf-8 -*-
module SeedExpress
  class Part
    module Delete
      def delete_missing_data
        delete_target_ids = seed_part.existing_ids - seed_part.new_ids
        callbacks[:before_deleting].call(delete_target_ids.size)
        if delete_target_ids.present?
          target_model.unscoped.where(:id => delete_target_ids).delete_all
        end
        callbacks[:after_deleting].call(delete_target_ids.size)
        delete_target_ids
      end

      def delete_waste_seed_records
        range_of_ids = seed_part.record_id_from .. seed_part.record_id_to
        master_record_ids = target_model.unscoped.where(:id => range_of_ids).pluck(:id)
        SeedRecord.delete_waste_digests!(seed_part.seed_table, range_of_ids, master_record_ids)
      end
    end
  end
end
