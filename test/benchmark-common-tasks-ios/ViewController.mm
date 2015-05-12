#import "ViewController.h"

#include "test_path.hpp"

extern "C" int benchmark_common_tasks_main(int, const char**);

@interface ViewController ()

@end

@implementation ViewController

- (void)viewDidLoad {
    [super viewDidLoad];
    
    NSString* tmpDir = NSTemporaryDirectory();
    std::string tmp_dir{tmpDir.UTF8String, tmpDir.length};
    realm::test_util::set_test_path_prefix(tmp_dir + '/');
    
    [[NSProcessInfo processInfo] performActivityWithOptions:NSActivityLatencyCritical reason:@"benchmark-common-tasks" usingBlock:^{
        benchmark_common_tasks_main(0, NULL);
    }];
    
    exit(0);
}

- (void)didReceiveMemoryWarning {
    [super didReceiveMemoryWarning];
    // Dispose of any resources that can be recreated.
}

@end
