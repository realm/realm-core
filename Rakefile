#!/usr/bin/env rake

require 'tmpdir'
require 'fileutils'

REALM_PROJECT_ROOT    = File.absolute_path(File.dirname(__FILE__))
REALM_BUILD_DIR_APPLE = "build.apple"
REALM_BUILD_DIR_STEM  = "build.make"

def generate_makefiles(build_dir)
    options = ENV.select {|k,_| k.start_with?("REALM_") || k.start_with?("CMAKE_") }.map{|k,v| "-D#{k}=#{v}"}.join(' ')

    Dir.chdir(build_dir) do
        sh "cmake -G\"Unix Makefiles\" #{REALM_PROJECT_ROOT} #{options}"
    end
end

REALM_CONFIGURATIONS = {
    debug:   'Debug',
    release: 'Release',
    cover:   'Debug',
}

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
        Dir.chdir("#{dir}/test") do
            sh "./realm-tests"
        end
    end

    desc "Run Valgrind for tests in #{configuration} mode"
    task "memcheck-#{configuration}" => "build-#{configuration}" do
        ENV['UNITTEST_THREADS'] ||= @num_processors
        Dir.chdir("#{dir}/test") do
            sh "valgrind #{@valgrind_flags} ./realm-tests"
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
task :build => ['build-debug', 'build-release']

desc 'Run tests in release mode'
task :check => 'check-release'

desc 'Run tests in debug mode under GDB'
task 'gdb-debug' => 'build-debug' do
    Dir.chdir("#{@build_dir}/test") do
        sh "gdb ./realm-tests"
    end
end

desc 'Run tests in debug mode under LLDB'
task 'lldb-debug' => 'build-debug' do
    Dir.chdir("#{@build_dir}/test") do
        sh "lldb ./realm-tests"
    end
end

desc 'Run coverage test and process output with LCOV'
task 'lcov' => ['check-cover', :tmpdir] do
    Dir.chdir("#{@build_dir}") do
        sh "lcov --capture --directory . --output-file #{@tmpdir}/realm.lcov"
        sh "lcov --extract #{@tmpdir}/realm.lcov '#{REALM_PROJECT_ROOT}/src/*' --output-file #{@tmpdir}/realm-clean.lcov"
        FileUtils.rm_rf "cover_html"
        sh "genhtml --prefix #{REALM_PROJECT_ROOT} --output-directory cover_html #{@tmpdir}/realm-clean.lcov"
    end
end

desc 'Run coverage test and process output with Gcovr'
task 'gcovr' => 'check-cover' do
    Dir.chdir(@build_dir) do
        sh "gcovr --filter='*src/realm.*' -x > gcovr.xml"
    end
end

task :asan_flags do
    ENV['ASAN_OPTIONS'] = "detect_odr_violation=2"
    ENV['EXTRA_CFLAGS'] = "-fsanitize=address"
    ENV['EXTRA_LDFLAGS'] = "-fsanitize=address"
end

desc 'Run address sanitizer in release mode.'
task 'asan' => [:asan_flags, 'check-release']

desc 'Run address sanitizer in debug mode.'
task 'asan-debug' => [:asan_flags, 'check-debug']

task :tsan_flags do
    ENV['EXTRA_CFLAGS'] = "-fsanitize=thread"
    ENV['EXTRA_LDFLAGS'] = "-fsanitize=thread"
end

desc 'Run thread sanitizer in release mode.'
task 'tsan' => [:tsan_flags, 'check-release']

desc 'Run thread sanitizer in debug mode.'
task 'tsan-debug' => [:tsan_flags, 'check-debug']

task :guess_version_string do
    major = nil
    minor = nil
    patch = nil
    File.open("CMakeLists.txt") do |f|
        f.each_line do |l|
            if l =~ /set\(REALM_VERSION_([A-Z]+)\s+(\d+)\)$/
                case $1
                when 'MAJOR' then major = $2
                when 'MINOR' then minor = $2
                when 'PATCH' then patch = $2
                end
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
    @build_dir = @jenkins_workspace or raise 'No WORKSPACE set.'
end

task :jenkins_flags => :jenkins_workspace do
    ENV['REALM_MAX_BPNODE_SIZE'] = '4'
end

desc 'Run by Jenkins as part of the core pipeline whenever master changes'
task 'jenkins-pipeline-unit-tests' => :jenkins_flags do
    ENV['UNITTEST_SHUFFLE'] = '1'
    ENV['UNITTEST_RANDOM_SEED'] = 'random'
    ENV['UNITTEST_XML'] = '1'
end

desc 'Run by Jenkins as part of the core pipeline whenever master changes'
task 'jenkins-pipeline-coverage' => [:jenkins_flags, :gcovr]

desc 'Run by Jenkins as part of the core pipeline whenever master changes'
task 'jenkins-pipeline-address-sanitizer' => [:jenkins_flags, 'asan-debug']

desc 'Run by Jenkins as part of the core pipeline whenever master changes'
task 'jenkins-pipeline-thread-sanitizer' => [:jenkins_flags, 'tsan-debug']

task :jenkins_valgrind_flags => :jenkins_flags do
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

REALM_COCOA_SUPPORTED_PLATFORMS = %w(macosx iphone watchos tvos)
if ENV['REALM_COCOA_PLATFORMS']
    REALM_COCOA_PLATFORMS = ENV['REALM_COCOA_PLATFORMS'].split
    REALM_COCOA_PLATFORMS.each do|p|
        unless REALM_COCOA_SUPPORTED_PLATFORMS.include?(p)
            $stderr.puts("Supported platforms are: #{REALM_COCOA_SUPPORTED_PLATFORMS.join(' ')}")
            exit 1
        end
    end
else
    REALM_COCOA_PLATFORMS = REALM_COCOA_SUPPORTED_PLATFORMS
end

apple_build_dir = ENV['build_dir'] || REALM_BUILD_DIR_APPLE

directory apple_build_dir

task :build_dir_apple => apple_build_dir do
    @build_dir = apple_build_dir
end

task :check_xcpretty do
    @xcpretty_suffix = `which xcpretty`.chomp
    @xcpretty_suffix = "| #{@xcpretty_suffix}" unless @xcpretty_suffix.empty?
end

desc 'Generate Xcode project (default dir: \'build.apple\')'
task :xcode_project => [:build_dir_apple, :check_xcpretty] do
    Dir.chdir(@build_dir) do
        sh "cmake -GXcode -DREALM_ENABLE_ENCRYPTION=1 -DREALM_ENABLE_ASSERTIONS=1 #{REALM_PROJECT_ROOT}"
    end
end

def build_apple(sdk, configuration, enable_bitcode, configuration_build_dir)
    Dir.chdir(@build_dir) do
        bitcode_mode   = configuration == 'Debug' ? 'marker' : 'bitcode'
        bitcode_option = "ENABLE_BITCODE=#{enable_bitcode ? "YES BITCODE_GENERATION_MODE=#{bitcode_mode}" : 'NO'}"
        configuration_temp_dir  = configuration_build_dir
        archs = case sdk
        when 'macosx', 'iphonesimulator' then "i386 x86_64"
        when 'appletvsimulator' then 'x86_64'
        when 'watchsimulator'   then 'i386'
        when 'iphoneos'  then 'armv7 armv7s arm64'
        when 'appletvos' then 'arm64'
        when 'watchos'   then 'armv7k'
        end
        sh <<-EOS.gsub(/\s+/, ' ')
            xcodebuild
            -sdk #{sdk}
            -target realm-static
            -configuration #{configuration}
            ARCHS=\"#{archs}\"
            ONLY_ACTIVE_ARCH=NO
            CONFIGURATION_BUILD_DIR=#{configuration_build_dir}
            CONFIGURATION_TEMP_DIR=#{configuration_temp_dir}
            #{bitcode_option}
            #{@xcpretty_suffix}
        EOS
    end
end

simulator_pairs = {
    'ios-bitcode' => ['iphoneos-bitcode', 'iphonesimulator-bitcode'],
    'ios-no-bitcode' => ['iphoneos', 'iphonesimulator'],
    'tvos' => ['appletvos', 'appletvsimulator'],
    'watchos' => ['watchos', 'watchsimulator']
}

['Debug', 'Release'].each do |configuration|
    target_suffix = configuration == 'Debug' ? '-dbg' : ''

    task "build-macosx#{target_suffix}" => :xcode_project do
        build_apple('macosx', configuration, false, "#{@build_dir}/build/#{configuration}-macosx")
    end

    ['watchos', 'appletvos', 'watchsimulator', 'appletvsimulator'].each do |sdk|
        task "build-#{sdk}#{target_suffix}" => :xcode_project do
            build_apple(sdk, configuration, true, "#{@build_dir}/build/#{configuration}-#{sdk}")
        end
    end

    [true, false].each do |enable_bitcode|
        bitcode_suffix = enable_bitcode ? '-bitcode' : ''
        ['iphoneos', 'iphonesimulator'].each do |sdk|
            task "build-#{sdk}#{bitcode_suffix}#{target_suffix}" => :xcode_project do
                build_apple(sdk, configuration, enable_bitcode, "#{@build_dir}/build/#{configuration}-#{sdk}#{bitcode_suffix}")
            end
        end
    end

    simulator_pairs.each do |target, pair|
        task "librealm-#{target}#{target_suffix}.a" => [:tmpdir_core, pair.map{|p| "build-#{p}#{target_suffix}"}].flatten do
            inputs = pair.map{|p| "#{@build_dir}/build/#{configuration}-#{p}/librealm.a"}
            output = "#{@tmpdir}/core/librealm-#{target}#{target_suffix}.a"
            sh "lipo -create -output #{output} #{inputs.join(' ')}"
        end
    end

    task "librealm-macosx#{target_suffix}.a" => [:tmpdir_core, "build-macosx#{target_suffix}"] do
        FileUtils.cp("#{@build_dir}/build/#{configuration}-macosx/librealm.a", "#{@tmpdir}/core/librealm-macosx#{target_suffix}.a")
        FileUtils.ln_s("librealm-macosx#{target_suffix}.a", "#{@tmpdir}/core/librealm#{target_suffix}.a")
    end
end

# alias to make CI happy. Ideally we should agree on a single name for iOS and use it everywhere.
task 'build-iphone' => 'build-iphoneos'

apple_static_library_targets = (['-dbg', ''].map do |dbg|
    REALM_COCOA_PLATFORMS.map {|p| p == 'iphone' ? ['ios-bitcode', 'ios-no-bitcode'] : p}.
        flatten.map {|c| "#{c}#{dbg}" }
end.flatten.map {|c| "librealm-#{c}.a" })

task :apple_static_libraries => apple_static_library_targets

task :tmpdir_core => :tmpdir do
    FileUtils.mkdir_p("#{@tmpdir}/core")
end

task :apple_copy_headers => :tmpdir_core do
    puts "Copying headers..."
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

task :apple_copy_license => :tmpdir_core do
    puts "Copying LICENSE..."
    FileUtils.cp("#{REALM_PROJECT_ROOT}/tools/LICENSE", "#{@tmpdir}/core/LICENSE")
end

task :check_pandoc do
    @pandoc = `which pandoc`.chomp
    if @pandoc.empty?
        $stderr.puts("Pandoc is required but it's not installed.  Aborting.")
        exit 1
    end
end

task :apple_release_notes => [:tmpdir_core, :check_pandoc] do
    sh "#{@pandoc} -f markdown -t plain -o \"#{@tmpdir}/core/release_notes.txt\" #{REALM_PROJECT_ROOT}/release_notes.md"
end

task :apple_zip => [:guess_version_string, :apple_static_libraries, :apple_copy_headers, :apple_copy_license, :apple_release_notes] do
    puts "Creating core-#{@version_string}.zip..."
    Dir.chdir(@tmpdir) do
        sh "zip -r -q --symlinks \"#{REALM_PROJECT_ROOT}/core-#{@version_string}.zip\" \"core\""
    end
end

desc 'Build zipped Core library suitable for Cocoa binding'
task 'build-cocoa' => :apple_zip do
    puts "TODO: Unzip in ../realm-cocoa"
end

