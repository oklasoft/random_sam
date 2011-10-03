#!/usr/bin/ruby1.9 -w
# encoding: UTF-8

#
# Copyright 2011 Oklahoma Medical Research Foundation. All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without modification, are
# permitted provided that the following conditions are met:
# 
#    1. Redistributions of source code must retain the above copyright notice, this list of
#       conditions and the following disclaimer.
# 
#    2. Redistributions in binary form must reproduce the above copyright notice, this list
#       of conditions and the following disclaimer in the documentation and/or other materials
#       provided with the distribution.
# 
# THIS SOFTWARE IS PROVIDED BY OKLAHOMA MEDICAL RESEARCH FOUNDATION AS IS'' AND ANY EXPRESS OR IMPLIED
# WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL OKLAHOMA MEDICAL RESEARCH FOUNDATION OR
# CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
# ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
# NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
# ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
# 
# The views and conclusions contained in the software and documentation are those of the
# authors and should not be interpreted as representing official policies, either expressed
# or implied, of Oklahoma Medical Research Foundation.
#

require 'wukong'

module SamFinalizer
  class Mapper < Wukong::Streamer::LineStreamer
    def process(line)
      parts = line.split(/\t+/)
      yield [ parts.shift, *parts]
    end
  end
  class Reducer < Wukong::Streamer::ListReducer
    def finalize
      values.each do |v|
        v.shift
        index = v.find_index("_SECOND_READ_")
        if index > 0
          yield v
        end
      end
    end
  end
end

Wukong::Script.new(SamFinalizer::Mapper,SamFinalizer::Reducer).run
