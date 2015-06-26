#import "ViewController.h"

#include "test_path.hpp"

extern "C" int benchmark_common_tasks_main();

@interface ViewController ()

@end

@implementation ViewController

- (void)viewDidLoad {
    [super viewDidLoad];
    
    std::string tmp_dir{[NSTemporaryDirectory() UTF8String]};
    realm::test_util::set_test_path_prefix(tmp_dir);

    std::string resource_path{[[[NSBundle mainBundle] resourcePath] UTF8String]};
    realm::test_util::set_test_resource_path(resource_path + "/");

    [[NSProcessInfo processInfo] performActivityWithOptions:NSActivityLatencyCritical reason:@"benchmark-common-tasks" usingBlock:^{
        benchmark_common_tasks_main();
    }];
    
    exit(0);
}

@end
