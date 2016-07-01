# seed_express

seed_express は ~~高速に~~ CSV を Database に登録する gem ライブラリです。<br/>
以下の特徴があります。

* Rails 3.0 以上対応
* CSV ファイルの中から、更新された部分のみを検知して、そこだけを DB に反映します。<br/>
  大量の更新はそれなりに時間を要しますが、更新が少ない場合は高速かつ安全です。
* CSV 中の行、列をコメントアウトして、特定のレコード、列の登録を行わないことが可能です。開発中に便利です。
* 通常の ActiveRecord の Validation が動作します。
* その他、特殊な Validation をサポートしています。


## インストール

次の行をアプリケーションの Gemfile に書き加えてください。

    gem 'seed_express'

そして以下を実行してください。

    $ bundle install

もしくは、自らインストールする場合、以下を実行してください。

    $ gem install seed_express

最後に、以下を実行してください。

    $ bundle exec rake db:migrate

### Rake タスクの作成

末尾に添付のような Rake タスクを作成して、アプリケーションのフォルダにおいてください。
この Rake タスクは将来は、 gem に内包する予定です。


### 処理対象テーブルの登録

\#{Rails.root}/db/master_table_list.rb というファイルを以下の様な内容で作成してください。<br/>
キーはテーブル名(Ruby シンボル型式)、値はとりあえず空ハッシュを指定してください。

    {
      :items       => {},
      :prefectures => {},
      :areas       => {},
    }

データ登録はここで記述した順番に行われます。
Validation の関係上、親子関係のあるテーブルは、子テーブルから登録をお願いします。

#### テーブルごとの動作のカスタマイズ
上記の空ハッシュの中で特定のキー(Ruby シンボル)を設定することで、テーブル毎に動作を変更することができます。

##### :nvl_mode
true を設定すると、 CSV 上でカラムに値が設定されていなかった場合にカラムの種類ごとに以下の動作をします。

  * String の場合、 ""(長さ 0 の文字列) を設定
  * Integer の場合、 0 を設定

##### :with_blanks
true を設定すると CSV を読み込んだ際に、カラム名および値の前後の空白を削除してから登録処理を行います。<br/>
CSV をスプレッドシートではなく、エディタで編集していた場合、列位置を揃えるために、空白が挿入されている場合がありました。<br/>
そのような CSV ファイルを処理できるようにするための機能です。

##### :parent_validation
特殊な Validation を行うための設定です。

例えば prefectures, cities といったような親子関係があり、
同じ prefecture 下の city が一つでも更新されたら他の city も validation を行いたい場合があるとします。
この時は以下のように設定します。

   :parent_validation => :prefectures

この設定により city が更新されると紐づく prefecture も更新されるようになり、
その結果 prefecture の Validation も実行されるため、
prefecture 下の cities をまとめての validation が実行されるようになります。

##### :filter_proc
この機能は将来廃止または、別の機能への代替を予定しています。<br/>
lamba 式を設定すると、テーブルに行が登録される前に呼び出されます。ここで登録する値を変更することが可能です。

引数として登録対象のレコードが Hash 型式で渡されます。<br/>
これを元に処理を行い、 Hash 型式で値を返すと、その返り値の Hash がレコードとして登録されます。

## 使い方

以下のように db:seed_express という Rake タスクを実行してください。

    rake db:seed_express


#### オプション

以下のようにオプションの指定が可能です。

    rake db:seed_express TABLES=table1,table2

##### TABLES オプション

    rake db:seed_express TABLES=table1,table2

登録対象のテーブルを指定してます。カンマ区切りで複数の指定が可能です。登録処理はここで記述した順に行われます。

##### TRUNCATE_MODE オプション

    rake db:seed_express TRUNCATE_MODE=true

上記のように設定すると、一旦テーブルの中身を削除して、まっさらな状態からデータ登録を行います。


##### FORCE_UPDATE_MODE オプション

    rake db:seed_express FORCE_UPDATE_MODE=true

TRUNCATE_MODE のように中身は削除しませんが、
テーブル上の全てのレコードを強制的に CSV の内容で更新します。

TRUNCATE_MODE では問題がある場合に使用します。
例えば一時的にテーブルが空になることで起こる問題を回避することができます。

seed_express はレコード毎に各列がどのような値かを digest 値を求めて記録しておき、次回の登録処理での処理削減に活用しています。
この情報が狂うと上手く登録が行われません。これを回避するためのオプションです。


## 動作原理
Comming soon...


## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request


### Rake タスク

以下のような rake タスクが必要です。
いずれ gem に取り込みます。

```ruby
# -*- coding: utf-8 -*-

namespace :db do
  desc "seed express  (params: TABLES=table1,table2,...  FORCE_UPDATE_MODE=true|TRUNCATE_MODE=true)"
  task :seed_express => :environment do
    begin
      target_csv_folder = "lib/tasks/csv"
      STDOUT.sync = true

      error = false
      filter_tables(master_tables).each_pair do |table_name, options|
        @showing_table = "%-36s ... " % table_name
        STDOUT.print @showing_table
        filter_each_lines = if options[:with_blanks]
                              filter_each_lines = ->(line) { line.chomp.gsub(/ *, */, ',') }
                            else
                              nil
                            end

        options = options.dup
        options[:filter_each_lines] = filter_each_lines
        options[:truncate_mode] = true if ENV.has_key?('TRUNCATE_MODE')
        options[:force_update_mode] = true if ENV.has_key?('FORCE_UPDATE_MODE')
        options[:datetime_offset] = 9.hours

        inserting_lambda = ->(part_count, part_total, record_count, record_total) { show_info("[#{part_count}/#{part_total}] inserting: #{record_count}/#{record_total}") }
        upating_lambda = ->(part_count, part_total, record_count, record_total)   { show_info("[#{part_count}/#{part_total}] updating: #{record_count}/#{record_total}") }

        making_bulk_digest_records_lambda = ->(record_count, record_total) { show_info("making bulk digest records: #{record_count}/#{record_total}") }
        upating_digests_lambda = ->(record_count, record_total)            { show_info("updating digests: #{record_count}/#{record_total}") }
        inserting_digests_lambda = ->(record_count, record_total)          { show_info("inserting digests: #{record_count}/#{record_total}") }

        options[:callbacks] = {
          :before_truncating                           => -> { show_info("truncating") },
          :before_reading_data                         => -> { show_info("reading") },
          :after_reading_data                          => ->(count) { show_info("read: #{count}") },
          :before_deleting                             => ->(count) { show_info("deleting: #{count}") },
          :after_deleting                              => ->(count) { show_info("deleted: #{count}") },
          :before_inserting_a_part                     => inserting_lambda,
          :after_inserting_a_part                      => inserting_lambda,
          :before_updating_a_part                      => upating_lambda,
          :after_updating_a_part                       => upating_lambda,
          :before_updating_digests                     => upating_digests_lambda,
          :before_updating_a_part_of_digests           => upating_digests_lambda,
          :after_updating_a_part_of_digests            => upating_digests_lambda,
          :before_making_bulk_digest_records           => making_bulk_digest_records_lambda,
          :before_making_a_part_of_bulk_digest_records => making_bulk_digest_records_lambda,
          :after_making_a_part_of_bulk_digest_records  => making_bulk_digest_records_lambda,
          :before_inserting_digests                    => inserting_digests_lambda,
          :before_inserting_a_part_of_digests          => inserting_digests_lambda,
          :after_inserting_a_part_of_digests           => inserting_digests_lambda,
        }

        seed_express = SeedExpress::CSV.new(table_name, target_csv_folder, options)
          out = seed_express.import
          case out[:result]
          when :skipped
            show_info("doesn't have any changes; skipped(elapsed time: %.2fsec.)\n" % out[:elapsed_time])
          when :error
            error = true
            show_info("errors have been detected(elapsed time: %.2fsec.)\n" % out[:elapsed_time])
          else
            show_info("inserted: %5d, updated:(prediction: %5d, actual: %5d), deleted: %5d, elapsed time: %.2fsec.\n" %
                      [
                       out[:inserted_count],
                       out[:updated_count], out[:actual_updated_count],
                       out[:deleted_count],
                       out[:elapsed_time],
                      ])
          end
      end
      if error
        raise "Errors have been detected on any tables"
      end
    end
  end
end

def show_info(msg)
  reset_line = "\x0d\x1b[K"
  STDOUT.print "#{reset_line}#{@showing_table}#{msg}"
end

def master_tables
  tables = nil
  File.open('db/master_table_list.rb') do |f|
    tables = eval(f.read)
  end

  tables
end

def filter_tables(master_tables)
  tables = ENV['TABLES'] || ENV['TABLE']
  return master_tables if tables.blank?
  tables = tables.split(",").map(&:strip)

  hash = {}
  tables.each do |table|
    hash[table.to_sym] = master_tables[table.to_sym]
  end
  hash
end
```

## TODO
* ファイル名に何用のファイルなのかといったコメントを書けるようにする。以下のように。

    `items.000001-0100000.古くてしばらく変更しないもの.csv`
    `items.000001-0100000.新しいもの.csv`

* ファイルの配置を指定ディレクトリの直下ではなく、サブディレクトリにも置けるようにする。
    * ジャンルごとの分類を可能にする。
