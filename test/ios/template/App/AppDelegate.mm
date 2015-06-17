//
//  AppDelegate.m
//  iOSTemp
//
//  Created by Oleks on 28/03/14.
//  Copyright (c) 2014 Realm. All rights reserved.
//

#import "AppDelegate.h"
#include "test_all.hpp"
#include "util/test_path.hpp"
#include <iostream>
#include <fstream>
#include <string>

using namespace realm::test_util;

@implementation AppDelegate

- (BOOL)application:(UIApplication *)application didFinishLaunchingWithOptions:(NSDictionary *)launchOptions
{
    self.window = [[UIWindow alloc] initWithFrame:[[UIScreen mainScreen] bounds]];
    // Override point for customization after application launch.
    self.window.backgroundColor = [UIColor whiteColor];
    [self.window makeKeyAndVisible];
    
    // Set the path prefix.
    string path_prefix = [NSTemporaryDirectory() UTF8String];
    set_test_path_prefix(path_prefix);

    // Set the resource path.
    string resource_path = ((string)[[[NSBundle mainBundle] resourcePath] UTF8String]) + "/";
    set_test_resource_path(resource_path);

    // Run the tests.
    test_all(0, NULL);
    
    // Report to stdout.
    std::cout << "====================" << std::endl;
    std::ifstream if_xml(path_prefix + "unit-test-report.xml", std::ios_base::binary);
    std::cout << if_xml.rdbuf();
    if_xml.close();
    std::cout << "====================" << std::endl;

    [[NSThread mainThread] cancel];
    return YES;
}

- (void)applicationWillResignActive:(UIApplication *)application
{
    // Sent when the application is about to move from active to inactive state. This can occur for certain types of temporary interruptions (such as an incoming phone call or SMS message) or when the user quits the application and it begins the transition to the background state.
    // Use this method to pause ongoing tasks, disable timers, and throttle down OpenGL ES frame rates. Games should use this method to pause the game.
}

- (void)applicationDidEnterBackground:(UIApplication *)application
{
    // Use this method to release shared resources, save user data, invalidate timers, and store enough application state information to restore your application to its current state in case it is terminated later. 
    // If your application supports background execution, this method is called instead of applicationWillTerminate: when the user quits.
}

- (void)applicationWillEnterForeground:(UIApplication *)application
{
    // Called as part of the transition from the background to the inactive state; here you can undo many of the changes made on entering the background.
}

- (void)applicationDidBecomeActive:(UIApplication *)application
{
    // Restart any tasks that were paused (or not yet started) while the application was inactive. If the application was previously in the background, optionally refresh the user interface.
}

- (void)applicationWillTerminate:(UIApplication *)application
{
    // Called when the application is about to terminate. Save data if appropriate. See also applicationDidEnterBackground:.
}

@end
