#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

include Java
java_import org.apache.hadoop.hbase.security.visibility.VisibilityClient
java_import org.apache.hadoop.hbase.security.visibility.VisibilityConstants
java_import org.apache.hadoop.hbase.utils.Bytes

module Hbase
  class VisibilityLabelsAdmin

    def initialize(admin)
      @admin = admin
      @config = @admin.getConfiguration()
    end

    def close
      @admin.close
    end

    def add_labels(*args)
      visibility_feature_available?
      # Normalize args
      if args.kind_of?(Array)
        labels = [ args ].flatten.compact
      end
      if labels.size() == 0
        raise(ArgumentError, "Arguments cannot be null")
      end

      begin
        response = VisibilityClient.addLabels(@config, labels.to_java(:string))
        if response.nil?
          raise(ArgumentError, "DISABLED: Visibility labels feature is not available")
        end
        labelsWithException = ""
        list = response.getResultList()
        list.each do |result|
            if result.hasException()
               labelsWithException += Bytes.toString(result.getException().getValue().toByteArray())
            end
        end    
        if labelsWithException.length > 0
          raise(ArgumentError, labelsWithException)
        end  
      end
    end

    def set_auths(user, *args)
      visibility_feature_available?
      # Normalize args
      if args.kind_of?(Array)
        auths = [ args ].flatten.compact
      end

      begin
        response = VisibilityClient.setAuths(@config, auths.to_java(:string), user)
        if response.nil?
          raise(ArgumentError, "DISABLED: Visibility labels feature is not available")
        end
        labelsWithException = ""
        list = response.getResultList()
        list.each do |result|
            if result.hasException()
               labelsWithException += Bytes.toString(result.getException().getValue().toByteArray())
            end
        end    
        if labelsWithException.length > 0
          raise(ArgumentError, labelsWithException)
        end
      end
    end

    def get_auths(user)
      visibility_feature_available?
      begin
        response = VisibilityClient.getAuths(@config, user)
        if response.nil?
          raise(ArgumentError, "DISABLED: Visibility labels feature is not available")
        end
        return response.getAuthList
      end
    end

    def list_labels(regex = ".*")
      visibility_feature_available?
      begin
        response = VisibilityClient.listLabels(@config, regex)
        if response.nil?
          raise(ArgumentError, "DISABLED: Visibility labels feature is not available")
        end
        return response.getLabelList
      end
    end

    def clear_auths(user, *args)
      visibility_feature_available?
      # Normalize args
      if args.kind_of?(Array)
        auths = [ args ].flatten.compact
      end

      begin
        response = VisibilityClient.clearAuths(@config, auths.to_java(:string), user)
        if response.nil?
          raise(ArgumentError, "DISABLED: Visibility labels feature is not available")
        end
        labelsWithException = ""
        list = response.getResultList()
        list.each do |result|
            if result.hasException()
               labelsWithException += Bytes.toString(result.getException().getValue().toByteArray())
            end
        end    
        if labelsWithException.length > 0
          raise(ArgumentError, labelsWithException)
        end
      end
    end

    # Make sure that lables table is available
    def visibility_feature_available?()
      caps = []
      begin
        # Try the getSecurityCapabilities API where supported.
        caps = @admin.getSecurityCapabilities
      rescue
        # If we are unable to use getSecurityCapabilities, fall back with a check for
        # deployment of the labels table
        raise(ArgumentError, "DISABLED: Visibility labels feature is not available") unless \
          exists?(VisibilityConstants::LABELS_TABLE_NAME)
        return
      end
      raise(ArgumentError, "DISABLED: Visibility labels feature is not available") unless \
        caps.include? org.apache.hadoop.hbase.client.security.SecurityCapability::CELL_VISIBILITY
    end

    # Does table exist?
    def exists?(table_name)
      @admin.tableExists(table_name)
    end
  end
end
