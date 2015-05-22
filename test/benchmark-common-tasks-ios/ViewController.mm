#import "ViewController.h"

#include "test_path.hpp"

extern "C" int benchmark_common_tasks_main();

@interface ViewController ()

@end

@implementation ViewController

- (void)viewDidLoad {
    [super viewDidLoad];
    
    NSString* tmpDir = NSTemporaryDirectory();
    std::string tmp_dir{tmpDir.UTF8String, tmpDir.length};
    realm::test_util::set_test_path_prefix(tmp_dir);
    
    [[NSProcessInfo processInfo] performActivityWithOptions:NSActivityLatencyCritical reason:@"benchmark-common-tasks" usingBlock:^{
        benchmark_common_tasks_main();
    }];
    
    exit(0);
}

@end
