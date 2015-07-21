#!/usr/bin/env ruby

base_dir = File.expand_path(File.join(File.dirname(__FILE__), ".."))
lib_dir = File.join(base_dir, "lib")
test_dir = File.join(base_dir, "test")

require "test-unit"
require "test/unit/rr"
require "codeclimate-test-reporter"

$LOAD_PATH.unshift(lib_dir)
$LOAD_PATH.unshift(test_dir)

ENV["TEST_UNIT_MAX_DIFF_TARGET_STRING_SIZE"] ||= "5000"

CodeClimate::TestReporter.start

exit Test::Unit::AutoRunner.run(true, test_dir)
