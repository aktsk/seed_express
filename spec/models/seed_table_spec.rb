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
end
