#!/usr/bin/env sh

cat >"$TEST_DIR/$APP.gyp" <<EOF
{
    'xcode_settings': {
        'ARCHS': [
            '\$(ARCHS_STANDARD_INCLUDING_64_BIT)',
        ],
        'SDKROOT': 'iphoneos',
        'TARGETED_DEVICE_FAMILY': '_BASENAME1,2', # iPhone/iPad
        'FRAMEWORK_SEARCH_PATHS': [
            '\$(SDKROOT)/Developer/Library/Frameworks',
            '\$(PROJECT_DIR)',
        ],
        'CODE_SIGN_IDENTITY[sdk=iphoneos*]': 'iPhone Developer',
        'CLANG_ENABLE_OBJC_ARC': 'YES',
        'OTHER_CPLUSPLUSFLAGS': [
            '\$(OTHER_CFLAGS)' ,
            '-DTIGHTDB_HAVE_CONFIG',
            $OTHER_CPLUSPLUSFLAGS
        ]
    },
    'target_defaults': {
        'link_settings': {
            'libraries': [
                '\$(SDKROOT)/usr/lib/libc++.dylib',
                '\$(DEVELOPER_DIR)/Library/Frameworks/XCTest.framework',
                '\$(SDKROOT)/System/Library/Frameworks/Foundation.framework',
                '\$(SDKROOT)/System/Library/Frameworks/CoreGraphics.framework',
                '\$(SDKROOT)/System/Library/Frameworks/UIKit.framework',
                '$FRAMEWORK',
            ],
        },
    },
    'targets': [
        {
            'target_name': '$APP',
            'type': 'executable',
            'mac_bundle': 1,
            'sources': [
                '$APP/AppDelegate.h',
                '$APP/AppDelegate.mm',
                '$APP/main.m',
$APP_SOURCES
            ],
            'mac_bundle_resources': [
                '$APP/Images.xcassets',
                '$APP/en.lproj/InfoPlist.strings',
                '$APP/$APP-Info.plist',
                '$APP/$APP-Prefix.pch',
                '$RESOURCES'
            ],
            'include_dirs': [
                '$TEST_APP/**'
            ],
            'xcode_settings': {
                'WRAPPER_EXTENSION': 'app',
                'INFOPLIST_FILE': '$APP/$APP-Info.plist',
                'GCC_PRECOMPILE_PREFIX_HEADER': 'YES',
                'GCC_PREFIX_HEADER': '$APP/$APP-Prefix.pch',
            }
        },
        {
            'target_name': '$TEST_APP',

            # see pylib/gyp/generator/xcode.py
            'type': 'loadable_module',
            'mac_xctest_bundle': 1,
            'sources': [
$TEST_APP_SOURCES
            ],
            'dependencies': [
                '$APP'
            ],
            'include_dirs': [
                '$TEST_APP/**'
            ],
            'xcode_settings': {
                'SDKROOT': 'iphoneos',
                'BUNDLE_LOADER': '\$(BUILT_PRODUCTS_DIR)/$APP.app/$APP',
                'TEST_HOST': '\$(BUNDLE_LOADER)',
            },
        },
    ],
}
EOF
