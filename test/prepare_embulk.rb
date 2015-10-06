module PrepareEmbulk
  require "embulk/command/embulk_run"

  if Embulk.respond_to?(:home)
    # keep compatibility for Embulk 0.6.x
    classpath_dir = Embulk.home("classpath")
    jars = Dir.entries(classpath_dir).select{|f| f =~ /\.jar$/ }.sort
    jars.each do |jar|
      require File.join(classpath_dir, jar)
    end
    require "embulk/java/bootstrap"
    require "embulk"
  else
    require "embulk"
    Embulk.setup
  end
end
