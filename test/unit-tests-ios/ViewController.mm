#import "ViewController.h"

#include "test_path.hpp"
#include "test_all.hpp"

@interface ViewController ()

@end

@implementation ViewController

- (void)viewDidLoad {
    [super viewDidLoad];
    NSString* tmpDir = NSTemporaryDirectory();
    std::string tmp_dir{tmpDir.UTF8String, tmpDir.length};
    realm::test_util::set_test_path_prefix(tmp_dir + '/');
    test_all(0, NULL);
    exit(0);
}

- (void)didReceiveMemoryWarning {
    [super didReceiveMemoryWarning];
    // Dispose of any resources that can be recreated.
}

@end
