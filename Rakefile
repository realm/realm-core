#!/usr/bin/env rake

require 'tmpdir'
require 'fileutils'

task :default do
  system("rake -sT")  # s for silent
end

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
        Dir.chdir("test") do
            sh "../#{dir}/test/realm-tests"
        end
    end

    desc "Run Valgrind for tests in #{configuration} mode"
    task "memcheck-#{configuration}" => "build-#{configuration}" do
        ENV['UNITTEST_THREADS'] ||= @num_processors
        Dir.chdir("test") do
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
        sh "gcovr --filter='.*src/realm.*' -x > gcovr.xml"
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

task :bpnode_size_4 do
    ENV['REALM_MAX_BPNODE_SIZE'] = '4'
end

task :jenkins_flags => [:jenkins_workspace, :bpnode_size_4]

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
    at_exit do
        FileUtils.remove_entry_secure @tmpdir
    end
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
REALM_DOTNET_COCOA_SUPPORTED_PLATFORMS = %w(iphone-no-bitcode)

def platforms(supported, requested)
    return supported unless requested

    requested = requested.gsub('ios', 'iphone').gsub(/\bosx\b/, 'macosx').split
    if (requested - supported).size > 0
        $stderr.puts("Supported platforms are: #{supported.join(' ')}")
        exit 1
    end
    requested
end

APPLE_BINDINGS = {
    'cocoa' => { :name => "Cocoa", :path => '../realm-cocoa/core',
                 :platforms => platforms(REALM_COCOA_SUPPORTED_PLATFORMS, ENV['REALM_COCOA_PLATFORMS']) },
    'dotnet-cocoa' => { :name => ".NET", :path => '../realm-dotnet/wrappers',
                        :platforms => platforms(REALM_DOTNET_COCOA_SUPPORTED_PLATFORMS, ENV['REALM_DOTNET_COCOA_PLATFORMS']) },
}

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
task 'xcode-project' => :xcode_project

task :xcode_project => [:build_dir_apple, :check_xcpretty] do
    Dir.chdir(@build_dir) do
        sh "cmake -GXcode -DREALM_ENABLE_ENCRYPTION=1 -DREALM_ENABLE_ASSERTIONS=1 #{REALM_PROJECT_ROOT}"
    end
end

def build_apple(sdk, configuration, bitcode: nil, install_to: nil)
    Dir.chdir(@build_dir) do
        bitcode_option = bitcode.nil? ? "" : "ENABLE_BITCODE=#{bitcode ? "YES" : "NO"}"
        install_action = install_to.nil? ? "" : "DSTROOT=#{install_to} install"

        sh <<-EOS.gsub(/\s+/, ' ')
            xcodebuild
            -sdk #{sdk}
            -target realm-static
            -configuration #{configuration}
            ONLY_ACTIVE_ARCH=NO
            #{bitcode_option}
            #{install_action}
            #{@xcpretty_suffix}
        EOS
    end
end

simulator_pairs = {
    'iphone' => ['iphoneos', 'iphonesimulator'],
    'iphone-no-bitcode' => ['iphoneos-no-bitcode', 'iphonesimulator-no-bitcode'],
    'tvos' => ['appletvos', 'appletvsimulator'],
    'watchos' => ['watchos', 'watchsimulator']
}

['Debug', 'Release'].each do |configuration|
    packaging_configuration = {'Debug' => 'MinSizeDebug', 'Release' => 'Release'}[configuration]
    target_suffix = configuration == 'Debug' ? '-dbg' : ''

    task "build-macosx#{target_suffix}" => :xcode_project do
        build_apple('macosx', configuration)
    end

    task "build-macosx#{target_suffix}-for-packaging" => :xcode_project do
        build_apple('macosx', packaging_configuration, install_to: "#{@tmpdir}/#{configuration}")
    end

    ['watchos', 'appletvos', 'watchsimulator', 'appletvsimulator'].each do |sdk|
        task "build-#{sdk}#{target_suffix}" => :xcode_project do
            build_apple(sdk, configuration)
        end

        task "build-#{sdk}#{target_suffix}-for-packaging" => :xcode_project do
            build_apple(sdk, packaging_configuration, install_to: "#{@tmpdir}/#{configuration}-#{sdk}")
        end
    end

    [true, false].each do |enable_bitcode|
        bitcode_suffix = enable_bitcode ? '' : '-no-bitcode'
        ['iphoneos', 'iphonesimulator'].each do |sdk|
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
        task "librealm-#{target}#{target_suffix}.a" => [:tmpdir_core, pair.map{|p| "build-#{p}#{target_suffix}-for-packaging"}].flatten do
            inputs = pair.map{|p| "#{@tmpdir}/#{configuration}-#{p}/librealm.a"}
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
        FileUtils.mv "#{@tmpdir}/#{zip_name}", "#{zip_name}"
    end

    desc "Build zipped Core library suitable for #{info[:name]} binding"
    task "build-#{name}" => "#{name}_zip" do
        puts "TODO: Unzip in #{info[:path]}"
    end
end


# Android

desc 'Build for Android'
task 'build-android' do
    puts "It's all good man!"
end
