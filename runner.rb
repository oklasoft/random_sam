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

ORIG_BASE = "/Volumes/hts_raw/scratch/20110930_random_bam_subset/"
NEEDED=22000000/2

# submitted 6

samples = %w/
  irf5_stim_3_input
  irf5_stim_2
  irf5_unstim_2
  irf5_unstim_3
  irf5_unstim_3_input
/

index = ENV['SGE_TASK_ID'].to_i - 1

slice_size = 1
if index < 0 
  slice_size = samples.size
  index = 0
end

samples = samples.slice(index,slice_size)

samples.each do |sample|
  puts sample
  base = "random_sam/#{sample}"
  randomized = "#{base}/randomized/"
  sorted = "#{base}/sorted/"

  # randomize  
  cmd = "/home/glennsb/src/random_sam/random_sam_subset.rb --run=hadoop --reduce_tasks=100 --limit=#{NEEDED} #{base}/input/ #{randomized}"
  puts cmd
  system(cmd)
  
  # finalize
  cmd = "/home/glennsb/src/random_sam/finalizer.rb --run=hadoop --reduce_tasks=100 #{randomized} #{sorted}"
  puts cmd
  system(cmd)
  
  Dir.mkdir(sample)
  Dir.chdir(sample) do
    new_file = "#{Dir.getwd}/#{sample}_random.sam"
    File.open(new_file,"w") do |output|
      IO.foreach("#{ORIG_BASE}/#{sample}.sam") do |line|
        break unless line =~ /^@/
        output.puts line.chomp
      end
    end
    tmp_out_file = "/tmp/#{$$}_#{sample}_sam_part.sam"
    
    added = 0
    File.open(new_file,"a") do |output|
      99999.times do |part|
        cmd = "hadoop fs -get #{sorted}part-#{part.to_s.rjust(5,"0")} #{tmp_out_file}"
        puts cmd
        system(cmd)
        IO.foreach(tmp_out_file) do |line|
          (read1,read2) = line.chomp.split(/\t_SECOND_READ_\t/)
          if read1 && read2
            output.puts read1
            output.puts read2
            added += 1
          end
          break if added >= NEEDED
        end
        File.unlink(tmp_out_file)
        break if added >= NEEDED     
      end
    end
    
  end #sample dir
  
  
  
end

