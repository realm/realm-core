#!/usr/bin/env sh

#cat >"$TEST_APP_DIR/$TEST_APP.mm" <<EOF
cat >"$TEST_DIR/$TEST_APP.mm" <<EOF
#import <XCTest/XCTest.h>
#include <sys/types.h>
#include <sys/sysctl.h>
#include <mach/machine.h>
#include "test_all.hpp"

@interface $TEST_APP : XCTestCase

@end

@implementation $TEST_APP

-(void)reportCPUType
{
    size_t size;
    cpu_type_t type;
    cpu_subtype_t subtype;
    size = sizeof(type);
    sysctlbyname("hw.cputype", &type, &size, NULL, 0);

    size = sizeof(subtype);
    sysctlbyname("hw.cpusubtype", &subtype, &size, NULL, 0);

    //printf("========== %d %d %d", type, CPU_TYPE_X86_64, CPU_TYPE_POWERPC64);
    printf("========== ");
    if (type == CPU_TYPE_X86 || type == CPU_TYPE_X86_64)
    {
        printf("x86");
    }
    else if (type == CPU_TYPE_ARM || type == CPU_TYPE_ARM64)
    {
        printf("arm %d", subtype);
        switch(subtype)
        {
        case CPU_SUBTYPE_ARM_V7:
            printf("v7");
            break;
        case CPU_SUBTYPE_ARM_V7S:
            printf("v7s");
            break;
        default:
            break;
        }
    } else {
        printf("(unrecognised cpu type)");
    }
    printf(" ==========\n");
}

-(void)testRunTests
{
    // Change working directory to somewhere we can write.
    [[NSFileManager defaultManager]
        changeCurrentDirectoryPath:(NSTemporaryDirectory())];
    
    [self reportCPUType];

    //test_all(0, NULL);

}

@end
EOF
