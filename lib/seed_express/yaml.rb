# -*- coding: utf-8 -*-

require 'yaml'

module SeedExpress
  class YAML < Abstract
    FILE_SUFFIX = 'yml'

    def read_values_from(data)
      ::YAML.load_stream(data)
    end
  end
end
