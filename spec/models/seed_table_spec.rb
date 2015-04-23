require File.expand_path(File.dirname(__FILE__) + '/../spec_helper')

describe SeedTable do
  describe "#get_record" do
    describe "about getting/creating a record" do
      context "when a specified record exists" do
        it "returns an existing seed_table record" do
          existing_record = SeedTable.create!(:table_name => 'items',
                                              :digest => 'ABC')
          record = SeedTable.get_record(:items)
          expect(record).to eq existing_record
        end
      end

      context "when a specified record doesn't exist" do
        it "returns a seed_table record which has been created" do
          SeedTable.delete_all
          record = SeedTable.get_record(:items)
          expect(record.new_record?).to be false
        end
      end
    end

    describe "about an argument" do
      context "when an argument is a String" do
        it "returns seed_table record" do
          record = SeedTable.get_record("items")
          expect(record.table_name).to eq "items"
        end
      end

      context "when an argument is a Symbol" do
        it "returns seed_table record" do
          record = SeedTable.get_record(:items)
          expect(record.table_name).to eq "items"
        end
      end

      context "when an argument is an ActiveRecord::Base object" do
        it "returns seed_table record" do
          record = SeedTable.get_record(Item.new)
          expect(record.table_name).to eq "items"
        end
      end

      context "when an argument is a Class object" do
        it "returns seed_table record" do
          record = SeedTable.get_record(Item)
          expect(record.table_name).to eq "items"
        end
      end
    end
  end

  describe "#disable_record_cache" do
    let(:seed_table) do
      seed_table = SeedTable.create!(:table_name => 'items',
                                     :digest => 'ABC')
      seed_table.disable_record_cache
      seed_table
    end

    describe "about setting SeedTable#digest" do

      it "becomes nil" do
        expect(seed_table.digest).to be_nil
      end

      it "saves the record" do
        expect(seed_table.reload.digest).to be_nil
      end
    end

    describe "about disabling SeedRecord#digest" do
      let(:seed_record_count) { 10 }
      before do
        seed_table.seed_records.delete_all
        @seed_records = (1 .. seed_record_count).map do |record_id|
          SeedRecord.create!(:seed_table_id => seed_table.id,
                             :record_id => record_id,
                             :digest => "D#{'%02d' % record_id}")
        end
      end

      context "when ids of an argument is present" do
        it "sets nil to SeedRecord#digest of specified seed_records" do
          split_index = 5
          record_ids = @seed_records[0, split_index].map(&:record_id)
          seed_table.disable_record_cache(record_ids)
          modified_digests = @seed_records[0, split_index].map(&:reload).map(&:digest)
          expect(modified_digests).to eq([nil] * split_index)

          original_digests = @seed_records[split_index, 999_999].map(&:digest)
          modified_digests =
            @seed_records[split_index, 999_999].map(&:reload).map(&:digest)
          expect(modified_digests).to eq original_digests
        end
      end

      context "when ids of an argument is blank" do
        it "sets nil to SeedRecord#digest of related seed_records" do
          seed_table.disable_record_cache
          modified_digests = @seed_records.map(&:reload).map(&:digest)
          expect(modified_digests).to eq ([nil] * seed_record_count)
        end
      end
    end
  end
end
