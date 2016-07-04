# -*- coding: utf-8 -*-
module SeedExpress
  class RubyHash < Abstract
    FILE_SUFFIX = 'rbhash'

    def read_values_from(data)
      eval(data)[:records]
    end
  end
end
