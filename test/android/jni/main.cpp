#include <jni.h>
#include <errno.h>
#include <iostream>
#include <fstream>
#include <string>

#include <android/log.h>
#include <android/native_activity.h>
#include <android_native_app_glue.h>
#include <android/asset_manager.h>

#include "../../test_all.hpp"
#include "../../util/test_path.hpp"

#define LOGI(...) ((void)__android_log_print(ANDROID_LOG_INFO, "native-activity", __VA_ARGS__))
#define LOGW(...) ((void)__android_log_print(ANDROID_LOG_WARN, "native-activity", __VA_ARGS__))


void android_main(struct android_app* state) 
{
    // Make sure glue isn't stripped.
    app_dummy();

    ANativeActivity* nativeActivity = state->activity;
    const char* externalDataPath = nativeActivity->externalDataPath;
    const char* internalDataPath = nativeActivity->internalDataPath;
    std::string inDataPath(internalDataPath);
    std::string exDataPath(externalDataPath);

    LOGI("Copying asset files...");
    const int buffer_size = 255;
    AAssetManager* assetManager = state->activity->assetManager;
    AAssetDir* assetDir = AAssetManager_openDir(assetManager, "");
    const char* filename = (const char*)NULL;
    while ((filename = AAssetDir_getNextFileName(assetDir)) != NULL) {
        LOGI("Asset file: %s", filename);
        AAsset* asset = AAssetManager_open(assetManager, filename, AASSET_MODE_STREAMING);
        char buf[buffer_size];
        int nb_read = 0;
        std::string filename_str(filename);
        std::string filepath = inDataPath + "/" + filename_str;
        FILE* out = fopen(filepath.c_str(), "w");
        while ((nb_read = AAsset_read(asset, buf, buffer_size)) > 0)
            fwrite(buf, nb_read, 1, out);
        fclose(out);
        AAsset_close(asset);
    }
    AAssetDir_close(assetDir);
    LOGI("Copying of asset files completed");

    tightdb::test_util::set_test_path_prefix(inDataPath + "/");
    tightdb::test_util::set_test_resource_path(inDataPath + "/");

    LOGI("Starting unit tests...");

    test_all(0,0);

    LOGI("Done running unit tests...");

    LOGI("Copying the test results in the external storage");
    std::string source = inDataPath + "/unit-test-report.xml";
    std::string destination = exDataPath + "/unit-test-report.xml";
    std::ifstream src(source.c_str(), std::ios::binary);
    std::ofstream dst(destination.c_str(), std::ios::binary);
    dst << src.rdbuf();
    LOGI("The XML file is located in %s", destination.c_str());

    ANativeActivity_finish(nativeActivity);
}
