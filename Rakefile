#!/usr/bin/env rake

require 'tmpdir'
require 'fileutils'

REALM_COCOA_SUPPORTED_PLATFORMS = %w(macosx iphoneos iphonesimulator watchos watchsimulator appletvos appletvsimulator)
if ENV['REALM_COCOA_PLATFORMS']
    REALM_COCOA_PLATFORMS = ENV['REALM_COCOA_PLATFORMS'].split.select {|p| REALM_COCOA_SUPPORTED_PLATFORMS.include?(p) }
else
    REALM_COCOA_PLATFORMS = REALM_COCOA_SUPPORTED_PLATFORMS
end

REALM_PROJECT_ROOT            = File.absolute_path(File.dirname(__FILE__))
REALM_DEFAULT_APPLE_BUILD_DIR = "build.apple"
REALM_DEFAULT_BUILD_DIR       = "build.make"

REALM_APPLE_BUILD_DIR = ENV['build_dir'] || REALM_DEFAULT_APPLE_BUILD_DIR
REALM_BUILD_DIR       = ENV['build_dir'] || REALM_DEFAULT_BUILD_DIR

directory REALM_BUILD_DIR
directory REALM_APPLE_BUILD_DIR unless REALM_APPLE_BUILD_DIR == REALM_BUILD_DIR

task :generate_makefiles => REALM_BUILD_DIR do
    options = ENV.select {|k,v| k.start_with?("REALM_")}.map{|k,v| "-D#{k}=#{v}"}.join

    Dir.chdir(REALM_BUILD_DIR) do
        sh "cmake -G\"Unix Makefiles\" #{REALM_PROJECT_ROOT} #{options}"
    end
end

task :build => :generate_makefiles do
    Dir.chdir(REALM_BUILD_DIR) do
        sh "cmake --build ."
    end
end

task 'build-debug' do
    ENV['REALM_DEBUG'] = '1'
    Rake::Task[:build].invoke
end

task :check => :build do
    Dir.chdir("#{REALM_BUILD_DIR}/test") do
        sh "./realm-tests"
    end
end

task 'check-debug' => 'build-debug' do
    Rake::Task[:check].execute
end

task 'gdb-debug' => 'build-debug' do
    Dir.chdir("#{REALM_BUILD_DIR}/test") do
        sh "gdb ./realm-tests"
    end
end

task 'lldb-debug' => 'build-debug' do
    Dir.chdir("#{REALM_BUILD_DIR}/test") do
        sh "lldb ./realm-tests"
    end
end

task :tmpdir do
    @tmpdir = Dir.mktmpdir("realm-build")
    puts "Using temporary directory: #{@tmpdir}"
end

task :check_xcpretty do
    @xcpretty_suffix = `which xcpretty`
    @xcpretty_suffix = "| #{@xcpretty_suffix}" unless @xcpretty_suffix.empty?
end

task :xcode_project => [REALM_APPLE_BUILD_DIR, :check_xcpretty] do
    Dir.chdir(REALM_APPLE_BUILD_DIR) do
        sh "cmake -GXcode #{REALM_PROJECT_ROOT}"
    end
end

def build_apple(sdk, configuration, enable_bitcode = false)
    Dir.chdir(REALM_APPLE_BUILD_DIR) do
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
                src = "#{REALM_APPLE_BUILD_DIR}/src/realm/#{configuration}#{platform_suffix}/librealm.a"
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

task 'build-cocoa' => :apple_zip

