diff --git a/src/realm/list.hpp b/src/realm/list.hpp
index 8813bda1c..219945fd5 100644
--- a/src/realm/list.hpp
+++ b/src/realm/list.hpp
@@ -144,19 +144,15 @@ public:
     template <typename Func>
     void find_all(value_type value, Func&& func) const
     {
-        if (update())
-        {
+        if (update()) {
             m_tree->find_all(value, std::forward<Func>(func));
-            if constexpr (std::is_same_v<T, Mixed>)
-            {
-                if (value.is_unresolved_link())
-                {
-                    //if value is a mixed which contains an unresolved link, find all the nulls
+            if constexpr (std::is_same_v<T, Mixed>) {
+                if (value.is_unresolved_link()) {
+                    // if value is a mixed which contains an unresolved link, find all the nulls
                     m_tree->find_all(realm::null(), std::forward<Func>(func));
                 }
-                else if (value.is_null())
-                {
-                    //if value is null then we find all the unresolved links with a linear scan
+                else if (value.is_null()) {
+                    // if value is null then we find all the unresolved links with a linear scan
                     find_all_mixed_unresolved_links(std::forward<Func>(func));
                 }
             }
@@ -274,15 +270,13 @@ protected:
         REALM_ASSERT(m_tree->is_attached());
         return true;
     }
-    
-    template<class Func>
+
+    template <class Func>
     void find_all_mixed_unresolved_links(Func&& func) const
     {
-        for(size_t i=0; i<m_tree->size(); ++i)
-        {
+        for (size_t i = 0; i < m_tree->size(); ++i) {
             auto mixed = m_tree->get(i);
-            if(mixed.is_unresolved_link())
-            {
+            if (mixed.is_unresolved_link()) {
                 func(i);
             }
         }
diff --git a/test/test_mixed_null_assertions.cpp b/test/test_mixed_null_assertions.cpp
index 1f9628d2f..6fdf6534f 100644
--- a/test/test_mixed_null_assertions.cpp
+++ b/test/test_mixed_null_assertions.cpp
@@ -125,11 +125,11 @@ TEST(Mixed_List_unresolved_as_null)
         CHECK(index == 0);
         index = list.find_first(obj1);
         CHECK(index == 2);
-        //but both should look like nulls
+        // but both should look like nulls
         CHECK(list.is_null(0));
         CHECK(list.is_null(2));
     }
-    
+
     {
         std::vector<size_t> indices{0, 1, 2};
         list.sort(indices);
