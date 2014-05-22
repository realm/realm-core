#!/usr/bin/env sh

cat >"$TEST_APP_DIR/$TEST_APP.mm" <<EOF
#import <XCTest/XCTest.h>
#include "test_all.hpp"

@interface $TEST_APP : XCTestCase

@end

@implementation $TEST_APP

-(void)testRunTests
{
    // Change working directory to somewhere we can write.
    [[NSFileManager defaultManager]
        changeCurrentDirectoryPath:(NSTemporaryDirectory())];
    test_all(0, NULL);
}

@end
EOF
