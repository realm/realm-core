#!/usr/bin/env rake

require 'tmpdir'
require 'fileutils'

REALM_PROJECT_ROOT            = File.absolute_path(File.dirname(__FILE__))
REALM_DEFAULT_BUILD_DIR_APPLE = "build.apple"
REALM_DEFAULT_BUILD_DIR_STEM  = "build.make"
REALM_DEFAULT_BUILD_DIR_DEBUG = "#{REALM_DEFAULT_BUILD_DIR_STEM}.debug"
REALM_DEFAULT_BUILD_DIR_OPTIM = "#{REALM_DEFAULT_BUILD_DIR_STEM}.release"
REALM_DEFAULT_BUILD_DIR_COVER = "#{REALM_DEFAULT_BUILD_DIR_STEM}.cover"

REALM_BUILD_DIR_APPLE = ENV['build_dir'] || REALM_DEFAULT_BUILD_DIR_APPLE
REALM_BUILD_DIR_DEBUG = ENV['build_dir'] || REALM_DEFAULT_BUILD_DIR_DEBUG
REALM_BUILD_DIR_OPTIM = ENV['build_dir'] || REALM_DEFAULT_BUILD_DIR_OPTIM
REALM_BUILD_DIR_COVER = ENV['build_dir'] || REALM_DEFAULT_BUILD_DIR_COVER

directory REALM_BUILD_DIR_APPLE

def generate_makefiles(build_dir)
    options = ENV.select {|k,_| k.start_with?("REALM_") || k.start_with?("CMAKE_") }.map{|k,v| "-D#{k}=#{v}"}.join(' ')

    Dir.chdir(build_dir) do
        sh "cmake -G\"Unix Makefiles\" #{REALM_PROJECT_ROOT} #{options}"
    end
end

REALM_CONFIGURATIONS = {
    debug:   ['Debug',   REALM_BUILD_DIR_DEBUG],
    release: ['Release', REALM_BUILD_DIR_OPTIM],
    cover:   ['Debug',   REALM_BUILD_DIR_COVER],
}

REALM_CONFIGURATIONS.each do |configuration, (build_type, dir)|
    directory dir

    task "config-#{configuration}" => dir do
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
        Dir.chdir("#{dir}/test") do
            sh "./realm-tests"
        end
    end

    desc "Run Valgrind for tests in #{configuration} mode"
    task "memcheck-#{configuration}" => "build-#{configuration}" do
        ENV['UNITTEST_THREADS'] ||= @num_processors
        Dir.chdir("#{dir}/test") do
            sh "valgrind ./realm-tests"
        end
    end
end

desc 'Build debug and release modes'
task :build => ['build-debug', 'build-release']

desc 'Run tests in release mode'
task :check => 'check-release'

desc 'Run tests in debug mode under GDB'
task 'gdb-debug' => 'build-debug' do
    Dir.chdir("#{REALM_BUILD_DIR_DEBUG}/test") do
        sh "gdb ./realm-tests"
    end
end

desc 'Run tests in debug mode under LLDB'
task 'lldb-debug' => 'build-debug' do
    Dir.chdir("#{REALM_BUILD_DIR_DEBUG}/test") do
        sh "lldb ./realm-tests"
    end
end

desc 'Run coverage test and process output with LCOV'
task 'lcov' => ['check-cover', :tmpdir] do
    Dir.chdir("#{REALM_BUILD_DIR_COVER}") do
        sh "lcov --capture --directory . --output-file #{@tmpdir}/realm.lcov"
        sh "lcov --extract #{@tmpdir}/realm.lcov '#{REALM_PROJECT_ROOT}/src/*' --output-file #{@tmpdir}/realm-clean.lcov"
        FileUtils.rm_rf "cover_html"
        sh "genhtml --prefix #{REALM_PROJECT_ROOT} --output-directory cover_html #{@tmpdir}/realm-clean.lcov"
    end
end

desc 'Forcibly remove all build state'
task :clean do
    REALM_CONFIGURATIONS.each do |name, (build_type, dir)|
        FileUtils.rm_rf(dir)
    end
    FileUtils.rm_rf(REALM_BUILD_DIR_APPLE)
end

task :tmpdir do
    @tmpdir = Dir.mktmpdir("realm-build")
    puts "Using temporary directory: #{@tmpdir}"
end

task :guess_operating_system do
    @operating_system = `uname`.chomp
end

task :guess_num_processors => :guess_operating_system do
    if @operating_system == 'Darwin'
        @num_processors = `sysctl -n hw.ncpu`.chomp
    else # assume Linux
        @num_processors = `cat /proc/cpuinfo | grep -E 'processor[[:space:]]*:' | wc -l`.chomp
    end
end


### Apple-specific tasks

REALM_COCOA_SUPPORTED_PLATFORMS = %w(macosx iphoneos iphonesimulator watchos watchsimulator appletvos appletvsimulator)
if ENV['REALM_COCOA_PLATFORMS']
    REALM_COCOA_PLATFORMS = ENV['REALM_COCOA_PLATFORMS'].split.select {|p| REALM_COCOA_SUPPORTED_PLATFORMS.include?(p) }
else
    REALM_COCOA_PLATFORMS = REALM_COCOA_SUPPORTED_PLATFORMS
end

task :check_xcpretty do
    @xcpretty_suffix = `which xcpretty`
    @xcpretty_suffix = "| #{@xcpretty_suffix}" unless @xcpretty_suffix.empty?
end

desc 'Generate Xcode project (default dir: \'build.apple\')'
task :xcode_project => [REALM_BUILD_DIR_APPLE, :check_xcpretty] do
    Dir.chdir(REALM_BUILD_DIR_APPLE) do
        sh "cmake -GXcode #{REALM_PROJECT_ROOT}"
    end
end

def build_apple(sdk, configuration, enable_bitcode = false)
    Dir.chdir(REALM_BUILD_DIR_APPLE) do
        bitcode_option = "ENABLE_BITCODE=#{enable_bitcode ? 'YES' : 'NO'}"
        sh "xcodebuild -sdk #{sdk} -target realm -configuration #{configuration} #{bitcode_option} #{@xcpretty_suffix}"
    end
end

def bitcode_configurations_for_platform(platform)
    case platform
    when 'macosx' then [false]
    when 'iphoneos' then [false] # FIXME
    else [true]
    end
end

REALM_COCOA_SUPPORTED_PLATFORMS.each do |platform|
    bitcode_configurations_for_platform(platform).each do |enable_bitcode|
        ["Debug", "Release"].each do |configuration|
            task "build_#{platform}_#{configuration}" => :xcode_project do
                build_apple(platform, configuration, enable_bitcode)
            end

            task "copy_static_library_#{platform}_#{configuration}" => ["build_#{platform}_#{configuration}", :tmpdir] do
                platform_suffix   = platform == 'macosx' ? '' : "-#{platform}"
                bitcode_suffix    = platform == 'macosx' ? '' : (enable_bitcode ? '-bitcode' : '-no-bitcode')
                dst_target_suffix = configuration == 'Debug'  ? "-dbg" : ''
                tag = "#{platform}#{bitcode_suffix}"
                src = "#{REALM_BUILD_DIR_APPLE}/src/realm/#{configuration}#{platform_suffix}/librealm.a"
                FileUtils.mkdir_p("#{@tmpdir}/core")
                dst = "#{@tmpdir}/core/librealm#{platform_suffix}#{bitcode_suffix}#{dst_target_suffix}.a"
                cp(src, dst)
                # FIXME: Combine static libraries in fat binaries.
            end
        end
    end
end

task :apple_static_libraries => ["Debug", "Release"].map{|c| REALM_COCOA_PLATFORMS.map{|p| "copy_static_library_#{p}_#{c}"}}.flatten

task :apple_copy_headers => :tmpdir do
    puts "Copying headers..."
    srcdir = "#{REALM_PROJECT_ROOT}/src"
    include_dir = "#{@tmpdir}/core/include"
    FileUtils.mkdir_p(include_dir)
    files_to_copy = Dir.glob("#{srcdir}/**/*.hpp") + Dir.glob("#{srcdir}/**/*.h")
    files_to_copy.each do |src|
        dst = src.sub(srcdir, include_dir)
        FileUtils.mkdir_p(File.dirname(dst))
        FileUtils.cp(src, dst)
    end
end

task :apple_copy_release_nodes_and_license => :tmpdir do
    puts "TODO: Copy release nodes and license"
end

task :apple_zip => [:apple_static_libraries, :apple_copy_headers, :apple_copy_release_nodes_and_license] do
    puts "Creating core-master.zip..."
    Dir.chdir(@tmpdir) do
        sh "zip -r -q --symlinks \"#{REALM_PROJECT_ROOT}/core-master.zip\" \"core\""
    end
end

desc 'Build zipped Core library suitable for Cocoa binding'
task 'build-cocoa' => :apple_zip do
    puts "TODO: Unzip in ../realm-cocoa"
end

