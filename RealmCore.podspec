Pod::Spec.new do |s|
  # Info
  s.name                = 'RealmCore'
  s.version             = `sh build.sh get-version`
  s.summary             = 'Realm Core'
  s.homepage            = 'https://realm.io'
  s.source              = { :git => 'https://github.com/realm/realm-core.git', :tag => "v#{s.version}" }
  s.author              = 'Realm'
  s.license             = 'Apache 2.0'

  # Compilation
  s.libraries           = 'c++'
  s.header_mappings_dir = 'src'
  s.source_files        = 'src/realm.hpp', 'src/realm/*.{h,hpp,cpp}', 'src/realm/{util,impl}/*.{h,hpp,cpp}'
  s.exclude_files       = 'src/realm/{config_tool,importer_tool,realmd,schema_dumper}.cpp'
  s.compiler_flags      = '-DREALM_ENABLE_ASSERTIONS',
                          '-DREALM_ENABLE_ENCRYPTION'
  s.pod_target_xcconfig = { 'APPLICATION_EXTENSION_API_ONLY' => 'YES',
                            'CLANG_CXX_LANGUAGE_STANDARD' => 'c++14',
                            'HEADER_SEARCH_PATHS' => '"$(PODS_ROOT)/RealmCore/src"' }

  # Platforms
  s.ios.deployment_target     = '7.0'
  s.osx.deployment_target     = '10.9'
  s.tvos.deployment_target    = '9.0'
  s.watchos.deployment_target = '2.0'
end
