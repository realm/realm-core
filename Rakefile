#!/usr/bin/env rake

require 'tmpdir'
require 'fileutils'
require 'uri'
require 'open-uri'

task :default do
  system('rake -sT') # s for silent
end

REALM_PROJECT_ROOT    = File.absolute_path(File.dirname(__FILE__))
REALM_BUILD_DIR_APPLE = 'build.apple'.freeze
REALM_BUILD_DIR_STEM  = 'build.make'.freeze
REALM_BUILD_DIR_ANDROID = 'build.android'.freeze

def generate_makefiles(build_dir)
  options = ENV.select { |k, _| k.start_with?('REALM_') || k.start_with?('CMAKE_') || k.start_with?('ANDROID_') }.map { |k, v| "-D#{k}=#{v}" }.join(' ')

  Dir.chdir(build_dir) do
    sh "cmake -G\"Unix Makefiles\" #{REALM_PROJECT_ROOT} #{options}"
  end
end

REALM_CONFIGURATIONS = {
  debug:   'Debug',
  release: 'Release',
  cover:   'Debug'
}.freeze

REALM_CONFIGURATIONS.each do |configuration, build_type|
  dir = ENV['build_dir'] || "#{REALM_BUILD_DIR_STEM}.#{configuration}"

  directory dir

  task "#{configuration}-build-dir" => dir do
    @build_dir ||= dir
  end

  task "config-#{configuration}" => "#{configuration}-build-dir" do
    ENV['CMAKE_BUILD_TYPE'] = build_type
    ENV['REALM_COVERAGE'] = '1' if configuration == :cover
    generate_makefiles(dir)
  end

  desc "Build in #{configuration} mode"
  task "build-#{configuration}" => ["config-#{configuration}", :guess_num_processors] do
    Dir.chdir(dir) do
      sh "cmake --build . -- -j#{@num_processors}"
    end
  end

  desc "Run tests in #{configuration} mode"
  task "check-#{configuration}" => "build-#{configuration}" do
    ENV['UNITTEST_THREADS'] ||= @num_processors
    Dir.chdir('test') do
      sh "../#{dir}/test/realm-tests"
    end
  end

  desc "Run Valgrind for tests in #{configuration} mode"
  task "memcheck-#{configuration}" => "build-#{configuration}" do
    ENV['UNITTEST_THREADS'] ||= @num_processors
    Dir.chdir('test') do
      sh "valgrind #{@valgrind_flags} ../#{dir}/test/realm-tests"
    end
  end
end

desc 'Produce Makefiles for debug and release.'
task :config, [:install_dir] do |_, args|
  install_dir = args[:install_dir] || '/usr/local'
  ENV['CMAKE_INSTALL_PREFIX'] = install_dir
  Rake::Task['config-debug'].invoke
  Rake::Task['config-release'].invoke
end

desc 'Build debug and release modes'
task build: ['build-debug', 'build-release']

desc 'Run tests in release mode'
task check: 'check-release'

desc 'Run tests in debug mode under GDB'
task 'gdb-debug' => 'build-debug' do
  Dir.chdir("#{@build_dir}/test") do
    sh 'gdb ./realm-tests'
  end
end

desc 'Run tests in debug mode under LLDB'
task 'lldb-debug' => 'build-debug' do
  Dir.chdir("#{@build_dir}/test") do
    sh 'lldb ./realm-tests'
  end
end

desc 'Run coverage test and process output with LCOV'
task 'lcov' => ['check-cover', :tmpdir] do
  Dir.chdir(@build_dir.to_s) do
    sh "lcov --capture --directory . --output-file #{@tmpdir}/realm.lcov"
    sh "lcov --extract #{@tmpdir}/realm.lcov '#{REALM_PROJECT_ROOT}/src/*' --output-file #{@tmpdir}/realm-clean.lcov"
    FileUtils.rm_rf 'cover_html'
    sh "genhtml --prefix #{REALM_PROJECT_ROOT} --output-directory cover_html #{@tmpdir}/realm-clean.lcov"
  end
end

desc 'Run coverage test and process output with Gcovr'
task 'gcovr' => 'check-cover' do
  Dir.chdir(@build_dir) do
    sh "gcovr --filter='.*src/realm.*' -x > gcovr.xml"
  end
end

task :afl_flags => :bpnode_size_4 do
    ENV['REALM_AFL'] = '1'
    ENV['REALM_ENABLE_ENCRYPTION'] = '1'
    # turn on all assertions explicity since
    # we build in release mode for speed
    ENV['REALM_ENABLE_ASSERTIONS'] = '1'
    ENV['REALM_DEBUG'] = '1'
end

task :afl_dir do
    if @build_dir.to_s.empty?
        @build_dir="build.make.release"
    end
    @afl_dir="#{@build_dir}/test/fuzzy/"
    puts "AFL commands are directed to: #{@afl_dir}"
end

desc 'Build and instrument the code for fuzz testing with AFL in release mode.'
task 'afl-build' => [:afl_flags, 'build-release', :afl_dir]

desc 'Start a fuzz test session, build and instrument the code if necessary.'
task 'afl-start' => 'afl-build' do
    Dir.chdir(@afl_dir) do
        sh "sh simple_start.sh"
    end
end

desc 'Check if there is a currently ongoing fuzz test and how many crashes it has found.'
task 'afl-status' => :afl_dir do
    Dir.chdir(@afl_dir) do
        sh "afl-whatsup findings/"
    end
end

desc 'Stop a fuzz test session and minimise results.'
task 'afl-stop' => :afl_dir do
    Dir.chdir(@afl_dir) do
        sh "sh simple_stop.sh"
    end
end

task :asan_flags do
  ENV['ASAN_OPTIONS'] = 'detect_odr_violation=2'
  ENV['EXTRA_CFLAGS'] = '-fsanitize=address'
  ENV['EXTRA_LDFLAGS'] = '-fsanitize=address'
end

desc 'Run address sanitizer in release mode.'
task 'asan' => [:asan_flags, 'check-release']

desc 'Run address sanitizer in debug mode.'
task 'asan-debug' => [:asan_flags, 'check-debug']

task :tsan_flags do
  ENV['EXTRA_CFLAGS'] = '-fsanitize=thread'
  ENV['EXTRA_LDFLAGS'] = '-fsanitize=thread'
end

desc 'Run thread sanitizer in release mode.'
task 'tsan' => [:tsan_flags, 'check-release']

desc 'Run thread sanitizer in debug mode.'
task 'tsan-debug' => [:tsan_flags, 'check-debug']

task :guess_version_string do
  major = nil
  minor = nil
  patch = nil
  File.open('CMakeLists.txt') do |f|
    f.each_line do |l|
      next unless l =~ /set\(REALM_VERSION_([A-Z]+)\s+(\d+)\)$/
      case Regexp.last_match(1)
      when 'MAJOR' then major = Regexp.last_match(2)
      when 'MINOR' then minor = Regexp.last_match(2)
      when 'PATCH' then patch = Regexp.last_match(2)
      end
    end
  end
  @version_string = "#{major}.#{minor}.#{patch}"
end

desc 'Print the version to stdout.'
task 'get-version' => :guess_version_string do
  puts @version_string
end

task :jenkins_workspace do
  raise 'No WORKSPACE set.' unless ENV['WORKSPACE']
  @jenkins_workspace = File.absolute_path(ENV['WORKSPACE'])
  (@build_dir = @jenkins_workspace) || raise('No WORKSPACE set.')
end

task :bpnode_size_4 do
  ENV['REALM_MAX_BPNODE_SIZE'] = '4'
  ENV['REALM_MAX_BPNODE_SIZE_DEBUG'] = '4'
end

task jenkins_flags: [:jenkins_workspace, :bpnode_size_4]

desc 'Run by Jenkins as part of the core pipeline whenever master changes'
task 'jenkins-pipeline-unit-tests' => :jenkins_flags do
  ENV['UNITTEST_SHUFFLE'] = '1'
  ENV['UNITTEST_RANDOM_SEED'] = 'random'
  ENV['UNITTEST_XML'] = '1'
end

desc 'Run by Jenkins as part of the core pipeline whenever master changes'
task 'jenkins-pipeline-coverage' => [:bpnode_size_4, :gcovr]

desc 'Run by Jenkins as part of the core pipeline whenever master changes'
task 'jenkins-pipeline-address-sanitizer' => [:jenkins_flags, 'asan-debug']

desc 'Run by Jenkins as part of the core pipeline whenever master changes'
task 'jenkins-pipeline-thread-sanitizer' => [:jenkins_flags, 'tsan-debug']

task jenkins_valgrind_flags: :jenkins_flags do
  ENV['REALM_ENABLE_ALLOC_SET_ZERO'] = '1'
  @valgrind_flags = "--tool=memcheck --leak-check=full --undef-value-errors=yes --track-origins=yes --child-silent-after-fork=no --trace-children=yes --xml=yes --xml-file=#{@jenkins_workspace}/realm-tests-dbg.%p.memreport"
end

task 'jenkins-valgrind' => [:jenkins_valgrind_flags, 'memcheck-release']

desc 'Forcibly remove all build state'
task :clean do
  REALM_CONFIGURATIONS.each do |configuration, _|
    FileUtils.rm_rf("#{REALM_BUILD_DIR_STEM}.#{configuration}")
  end
  FileUtils.rm_rf(REALM_BUILD_DIR_APPLE)
end

task :tmpdir do
  @tmpdir = Dir.mktmpdir('realm-build')
  at_exit do
    FileUtils.remove_entry_secure @tmpdir
  end
  puts "Using temporary directory: #{@tmpdir}"
end

task :guess_operating_system do
  @operating_system = `uname`.chomp
end

task guess_num_processors: :guess_operating_system do
  if @operating_system == 'Darwin'
    @num_processors = `sysctl -n hw.ncpu`.chomp
  else # assume Linux
    @num_processors = `cat /proc/cpuinfo | grep -E 'processor[[:space:]]*:' | wc -l`.chomp
  end
end

### Apple-specific tasks

REALM_COCOA_SUPPORTED_PLATFORMS = %w(macosx iphone watchos tvos).freeze
REALM_DOTNET_COCOA_SUPPORTED_PLATFORMS = %w(iphone-no-bitcode).freeze

def platforms(supported, requested)
  return supported unless requested

  requested = requested.gsub('ios', 'iphone').gsub(/\bosx\b/, 'macosx').split
  unless (requested - supported).empty?
    $stderr.puts("Supported platforms are: #{supported.join(' ')}")
    exit 1
  end
  requested
end

APPLE_BINDINGS = {
  'cocoa' => { name: 'Cocoa', path: '../realm-cocoa/core',
               platforms: platforms(REALM_COCOA_SUPPORTED_PLATFORMS, ENV['REALM_COCOA_PLATFORMS']) }
}.freeze

apple_build_dir = ENV['build_dir'] || REALM_BUILD_DIR_APPLE

directory apple_build_dir

task build_dir_apple: apple_build_dir do
  @build_dir = apple_build_dir
end

task :check_xcpretty do
  @xcpretty_suffix = `which xcpretty`.chomp
  @xcpretty_suffix = "| #{@xcpretty_suffix}" unless @xcpretty_suffix.empty?
end

desc 'Generate Xcode project (default dir: \'build.apple\')'
task 'xcode-project' => :xcode_project

task xcode_project: [:build_dir_apple, :check_xcpretty] do
  Dir.chdir(@build_dir) do
    sh "cmake -GXcode -DREALM_ENABLE_ENCRYPTION=1 -DREALM_ENABLE_ASSERTIONS=1 -DREALM_LIBTYPE=STATIC #{REALM_PROJECT_ROOT}"
  end
end

def build_apple(sdk, configuration, bitcode: nil, install_to: nil)
  Dir.chdir(@build_dir) do
    bitcode_option = bitcode.nil? ? '' : "ENABLE_BITCODE=#{bitcode ? 'YES' : 'NO'}"
    install_action = install_to.nil? ? '' : "DSTROOT=#{install_to} install"

    sh <<-EOS.gsub(/\s+/, ' ')
            xcodebuild
            -sdk #{sdk}
            -target realm
            -configuration #{configuration}
            ONLY_ACTIVE_ARCH=NO
            #{bitcode_option}
            #{install_action}
            #{@xcpretty_suffix}
        EOS
  end
end

simulator_pairs = {
  'iphone' => %w(iphoneos iphonesimulator),
  'iphone-no-bitcode' => ['iphoneos-no-bitcode', 'iphonesimulator-no-bitcode'],
  'tvos' => %w(appletvos appletvsimulator),
  'watchos' => %w(watchos watchsimulator)
}

%w(Debug Release).each do |configuration|
  packaging_configuration = { 'Debug' => 'MinSizeDebug', 'Release' => 'Release' }[configuration]
  target_suffix = configuration == 'Debug' ? '-dbg' : ''

  task "build-macosx#{target_suffix}" => :xcode_project do
    build_apple('macosx', configuration)
  end

  task "build-macosx#{target_suffix}-for-packaging" => :xcode_project do
    build_apple('macosx', packaging_configuration, install_to: "#{@tmpdir}/#{configuration}")
  end

  %w(watchos appletvos watchsimulator appletvsimulator).each do |sdk|
    task "build-#{sdk}#{target_suffix}" => :xcode_project do
      build_apple(sdk, configuration)
    end

    task "build-#{sdk}#{target_suffix}-for-packaging" => :xcode_project do
      build_apple(sdk, packaging_configuration, install_to: "#{@tmpdir}/#{configuration}-#{sdk}")
    end
  end

  [true, false].each do |enable_bitcode|
    bitcode_suffix = enable_bitcode ? '' : '-no-bitcode'
    %w(iphoneos iphonesimulator).each do |sdk|
      task "build-#{sdk}#{bitcode_suffix}#{target_suffix}" => :xcode_project do
        build_apple(sdk, configuration, bitcode: enable_bitcode)
      end

      task "build-#{sdk}#{bitcode_suffix}#{target_suffix}-for-packaging" => :xcode_project do
        build_apple(sdk, packaging_configuration, bitcode: enable_bitcode,
                                                  install_to: "#{@tmpdir}/#{configuration}-#{sdk}#{bitcode_suffix}")
      end
    end
  end

  simulator_pairs.each do |target, pair|
    task "librealm-#{target}#{target_suffix}.a" => [:tmpdir_core, pair.map { |p| "build-#{p}#{target_suffix}-for-packaging" }].flatten do
      inputs = pair.map { |p| "#{@tmpdir}/#{configuration}-#{p}/librealm.a" }
      output = "#{@tmpdir}/core/librealm-#{target}#{target_suffix}.a"
      sh "lipo -create -output #{output} #{inputs.join(' ')}"
    end
  end

  task "librealm-macosx#{target_suffix}.a" => [:tmpdir_core, "build-macosx#{target_suffix}-for-packaging"] do
    FileUtils.mv("#{@tmpdir}/#{configuration}/librealm.a", "#{@tmpdir}/core/librealm-macosx#{target_suffix}.a")
    FileUtils.ln_s("librealm-macosx#{target_suffix}.a", "#{@tmpdir}/core/librealm#{target_suffix}.a")
  end
end

task 'build-iphone' => ['build-iphoneos', 'build-iphonesimulator']
task 'build-ios' => 'build-iphone'

task tmpdir_core: :tmpdir do
  FileUtils.mkdir_p("#{@tmpdir}/core")
end

task apple_copy_headers: :tmpdir_core do
  puts 'Copying headers...'
  srcdir = "#{REALM_PROJECT_ROOT}/src"
  include_dir = "#{@tmpdir}/core/include"
  FileUtils.mkdir_p(include_dir)
  # FIXME: Get the real install-headers from CMake, somehow.
  files_to_copy = Dir.glob("#{srcdir}/**/*.hpp") + Dir.glob("#{srcdir}/**/*.h")
  gendir = "#{@build_dir}/src"
  files_to_copy += [
    "#{@build_dir}/src/realm/version.hpp",
    "#{@build_dir}/src/realm/util/config.h"
  ]
  files_to_copy.reject! { |f| f =~ /win32/ }
  files_to_copy.each do |src|
    dst = src.sub(gendir, include_dir)
    dst = dst.sub(srcdir, include_dir)
    FileUtils.mkdir_p(File.dirname(dst))
    FileUtils.cp(src, dst)
  end
end

task apple_copy_license: :tmpdir_core do
  puts 'Copying LICENSE...'
  FileUtils.cp("#{REALM_PROJECT_ROOT}/tools/LICENSE", "#{@tmpdir}/core/LICENSE")
end

task :check_pandoc do
  @pandoc = `which pandoc`.chomp
  if @pandoc.empty?
    $stderr.puts("Pandoc is required but it's not installed.  Aborting.")
    exit 1
  end
end

task apple_release_notes: [:tmpdir_core, :check_pandoc] do
  sh "#{@pandoc} -f markdown -t plain -o \"#{@tmpdir}/core/release_notes.txt\" #{REALM_PROJECT_ROOT}/CHANGELOG.md"
end

APPLE_BINDINGS.map do |name, info|
  static_library_targets = info[:platforms].product(['-dbg', '']).map do |platform, suffix|
    "librealm-#{platform}#{suffix}.a"
  end.to_a

  task "#{name}_static_libraries" => static_library_targets

  task "#{name}_zip" => [:guess_version_string, "#{name}_static_libraries", :apple_copy_headers, :apple_copy_license, :apple_release_notes] do
    zip_name = "core-#{@version_string}.zip"
    puts "Creating #{zip_name}..."
    Dir.chdir(@tmpdir) do
      sh "zip -r -q --symlinks \"#{zip_name}\" \"core\""
    end
    FileUtils.mv "#{@tmpdir}/#{zip_name}", zip_name.to_s
  end

  desc "Build zipped Core library suitable for #{info[:name]} binding"
  task "build-#{name}" => "#{name}_zip" do
    puts "TODO: Unzip in #{info[:path]}"
  end
end

# Android

android_build_types = %w(Release Debug)

android_abis = [
  { :name => 'armeabi-v7a', :package_name => 'arm-v7a' },
  { :name => 'x86', :package_name => 'x86' },
  { :name => 'mips', :package_name => 'mips' },
  { :name => 'arm64-v8a', :package_name => 'arm64' },
  { :name => 'x86_64', :package_name => 'x86_64' }
]

build_android_dependencies = []

android_abis.product(android_build_types) do |abi, build_type|
  dir = ENV['build_dir'] || "#{REALM_BUILD_DIR_ANDROID}.#{abi[:name]}.#{build_type}"
  directory dir

  desc "Configure the Android build in #{build_type} mode for #{abi[:name]}"
  task "config-android-#{abi[:name]}-#{build_type}" => [dir] do
    ENV['CMAKE_TOOLCHAIN_FILE'] = 'tools/cmake/android.toolchain.cmake'
    ENV['REALM_PLATFORM'] = 'Android'
    ENV['CMAKE_INSTALL_PREFIX'] = 'install'
    ENV['CMAKE_BUILD_TYPE'] = build_type
    ENV['ANDROID_ABI'] = abi[:name]
    ENV['REALM_ENABLE_ENCRYPTION'] = '1'
    generate_makefiles(dir)
  end

  desc "Build the core static library in #{build_type} mode for #{abi[:name]}"
  task "build-android-#{abi[:name]}-#{build_type}" => ["config-android-#{abi[:name]}-#{build_type}", 'guess_num_processors'] do
    Dir.chdir(dir) do
      sh "make realm -j#{@num_processors}"
      sh 'make install/fast'
    end
  end

  desc "Build the core tests in #{build_type} mode for #{abi[:name]}"
  task "build-tests-android-#{abi[:name]}-#{build_type}" => ["config-android-#{abi[:name]}-#{build_type}", 'guess_num_processors'] do
    Dir.chdir(dir) do
      sh "make all -j#{@num_processors}"
    end
  end

  build_android_dependencies << "build-android-#{abi[:name]}-#{build_type}"
  abi[:"filename_#{build_type.downcase}"] = "#{dir}/install/lib/librealm.a"
  abi[:"build_dir_#{build_type.downcase}"] = dir
end

desc 'Build for Android'
task 'build-android' => build_android_dependencies

android_package_dependencies = android_abis.map { |abi| [ abi[:filename_release], abi[:filename_debug] ] }.flatten

desc 'Build the package for Android'
task 'package-android' => android_package_dependencies do
  Dir.mktmpdir do |dir|
    android_abis.each do |abi|
      FileUtils.cp(abi[:filename_release], "#{dir}/librealm-android-#{abi[:package_name]}.a")
      FileUtils.cp(abi[:filename_debug], "#{dir}/librealm-android-#{abi[:package_name]}-dbg.a")
    end
    FileUtils.copy_entry("#{android_abis.first[:build_dir_release]}/install/include", "#{dir}/include")
    File.open("#{dir}/OpenSSL.txt", 'a') do |file|
      file << "This product includes software developed by the OpenSSL Project for use in the OpenSSL toolkit. (http://www.openssl.org/).\n\n"
      file << "The following license applies only to the portions of this product developed by the OpenSSL Project.\n\n"
      file << open('https://raw.githubusercontent.com/openssl/openssl/master/LICENSE').read
    end
    Dir.chdir(dir) do
      sh "cmake -E tar czf #{__dir__}/realm-core-android-latest.tar.gz *"
    end
  end
end
