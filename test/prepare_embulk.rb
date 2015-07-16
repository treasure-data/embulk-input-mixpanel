require "embulk/command/embulk_run"

classpath_dir = Embulk.home("classpath")
jars = Dir.entries(classpath_dir).select{|f| f =~ /\.jar$/ }.sort
jars.each do |jar|
  require File.join(classpath_dir, jar)
end
require "embulk/java/bootstrap"

require "embulk"
