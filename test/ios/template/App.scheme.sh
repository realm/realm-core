#!/usr/bin/env sh

cat >"$TEST_DIR/$APP.xcodeproj/xcuserdata/$USER.xcuserdatad/xcschemes/$APP.xcscheme" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<Scheme
   LastUpgradeVersion = "0500"
   version = "1.3">
   <TestAction
      selectedDebuggerIdentifier = "Xcode.DebuggerFoundation.Debugger.LLDB"
      selectedLauncherIdentifier = "Xcode.DebuggerFoundation.Launcher.LLDB"
      shouldUseLaunchSchemeArgsEnv = "YES"
      buildConfiguration = "Default">
      <Testables>
         <TestableReference
            skipped = "NO">
            <BuildableReference
               BuildableIdentifier = "primary"
               BlueprintIdentifier = "$TEST_APP_ID"
               BuildableName = "$TEST_APP.xctest"
               BlueprintName = "$TEST_APP"
               ReferencedContainer = "container:$APP.xcodeproj">
            </BuildableReference>
         </TestableReference>
      </Testables>
      <MacroExpansion>
         <BuildableReference
            BuildableIdentifier = "primary"
            BlueprintIdentifier = "$APP_ID"
            BuildableName = "$APP.app"
            BlueprintName = "$APP"
            ReferencedContainer = "container:$APP.xcodeproj">
         </BuildableReference>
      </MacroExpansion>
   </TestAction>
</Scheme>
EOF
