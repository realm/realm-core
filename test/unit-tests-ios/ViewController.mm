#import "ViewController.h"

#include "test_path.hpp"
#include "test_all.hpp"

@interface ViewController ()

@end

@implementation ViewController

- (void)viewDidLoad {
    [super viewDidLoad];

    std::string tmp_dir{[NSTemporaryDirectory() UTF8String]};
    realm::test_util::set_test_path_prefix(tmp_dir);

    std::string resource_path{[[[NSBundle mainBundle] resourcePath] UTF8String]};
    realm::test_util::set_test_resource_path(resource_path + "/");

    test_all(0, nullptr);
    exit(0);
}

@end
