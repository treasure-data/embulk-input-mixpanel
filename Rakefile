require "bundler/gem_tasks"
require "everyleaf/embulk_helper/tasks"

task default: :test

desc "Run tests"
task :test do
  ruby("test/run-test.rb", "--use-color=yes", "--collector=dir")
end

Everyleaf::EmbulkHelper::Tasks.install(
  gemspec: "./embulk-input-mixpanel.gemspec",
  github_name: "treasure-data/embulk-input-mixpanel",
)
