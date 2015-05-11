//
//  ViewController.m
//  benchmark-common-tasks-ios
//
//  Created by Simon Ask Ulsnes on 11/05/15.
//  Copyright (c) 2015 Realm. All rights reserved.
//

#import "ViewController.h"

#include "test_path.hpp"

extern "C" int benchmark_common_tasks_main(int, const char**);

@interface ViewController ()

@end

@implementation ViewController

- (void)viewDidLoad {
    [super viewDidLoad];
    NSURL* tmpUrl = [[NSFileManager defaultManager] URLsForDirectory:NSDocumentDirectory inDomains:NSUserDomainMask].firstObject;
    NSString* tmpDir = tmpUrl.path;
    std::string tmp_dir{tmpDir.UTF8String, tmpDir.length};
    realm::test_util::set_test_path_prefix(tmp_dir + '/');
    benchmark_common_tasks_main(0, NULL);
    exit(0);
}

- (void)didReceiveMemoryWarning {
    [super didReceiveMemoryWarning];
    // Dispose of any resources that can be recreated.
}

@end
