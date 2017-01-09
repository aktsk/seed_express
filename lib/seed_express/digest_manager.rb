# -*- coding: utf-8 -*-
module SeedExpress
  module DigestManager
    private

    def do_truncate_or_force_update
      if truncate_mode
        truncate_table
      elsif need_disabling_digest?
        disable_digests
      end
    end

    def need_disabling_digest?
      force_update_mode || seed_table.schema_updated?
    end

    def truncate_table
      callbacks[:before_truncating].call
      ActiveRecord::Base.transaction do
        seed_table.truncate_digests
      end

      target_model.connection.execute("TRUNCATE TABLE #{@table_name}")
      callbacks[:after_truncating].call
    end

    def disable_digests
      callbacks[:before_disabling_digests].call
      ActiveRecord::Base.transaction do
        seed_table.disable_record_digests
      end
      callbacks[:before_disabling_digests].call
    end

    def renew_digests(r, has_an_error)
      return if has_an_error
      ActiveRecord::Base.transaction do
        seed_table.seed_records.renew_digests!(self, r[:inserted_ids], r[:updated_ids], r[:digests])
        self.parts.renew_digests!
        seed_table.renew_digest!
      end
    end

    def update_parent_digest_to_validate(args)
      return unless self.parent_validation
      parent_table = self.parent_validation
      parent_table_model = ModelClass.from_table(parent_table)
      SeedTable.get_record(parent_table_model).disable_record_digests(parent_ids(args))
    end

    def parent_ids(args)
      parent_table = self.parent_validation
      parent_id_column = (parent_table.to_s.singularize + "_id").to_sym
      target_model.unscoped.where(:id => args[:inserted_ids] + args[:updated_ids]).
        group(parent_id_column).pluck(parent_id_column)
    end
  end
end
