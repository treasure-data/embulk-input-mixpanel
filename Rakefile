require "bundler/gem_tasks"

task default: :build

desc "Run tests"
task :test do
  ruby("test/run-test.rb", "--use-color=yes", "--collector=dir")
end
