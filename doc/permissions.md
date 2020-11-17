Finer-Grained Permissions via ACLs
==================================

Permissions are implemented by adding an "ACL property" to objects. The ACL
property is internally a link list containing links to the special
`__Permission` objects. The basic structure is the same for Realm, Class, or
Object level permissions. The admin token acts as the master key such that if a
developer messes up the permissions restricting their access in a way, the admin
token is able to change all data for all Realms.

Per-object permissions are only in effect under "Partial Sync". In
complete-replica Sync, permissions have no effect. This is because the
facilities of partial sync are used by the permission system to set boundaries
on the client's view of the Realm file.

Permissions in Realm are “white-lists”, which means that a completely empty
Realm file with no users, roles, or permission info is totally locked down and
inaccessible through partial sync. By default, when the server creates a new
Realm file per the request of a user, the server will insert a very permissive
set of “default” permissions, where the special role called “everyone” has
access to everything in the Realm file (including modifying the schema and
setting permissions).

When a new user connects to the Realm file, the server adds an entry in that
Realm file for the user, and adds that user as a member of the “everyone” role.

When the app developer is ready, they can create new roles (such as an “admin”
role) and lower the privileges of the “everyone” role as appropriate for their
app.


Object Permissions
------------------

Object Permissions work by adding a property to the class of type
`LinkList<__Permission>`, which works as an ACL. Setting permissions for an
object means creating a `__Permission` object, populating it with the relevant
privileges, assign it to a role, and adding it to the ACL property on the
object.

Objects can have the following permissions:

- _Read:_ The role can read and see the object (i.e. the object may be included
  in query results).
- _Update:_ The role can modify all properties on the object except the
  permission property.
- _Delete:_ The role can delete the object.
- _Share_ and _SetPermissions:_ The role can modify the permissions property on
  the object. The two names are synonyms.

When a class does not have an ACL property, all objects in that class are
accessible by everyone. When an object’s ACL is empty, nobody can access the
object. An object’s ACL is always modifiable by its creator in the same write
transaction as the object was created.

**NOTE ABOUT TRANSITIVE CLOSURE:** Due to the lack of support for “opaque links”
or “asynchronous links” in Core, we must synchronize a full object graph to the
client. This means that if a client has access to some part of an object graph
but not the whole graph, we must choose to either include the whole graph or
nothing. We have chosen the former (what we call the “transitive closure”),
meaning that every object that is reachable through links will be visible to the
user regardless of permissions. Write permissions can still be enforced.


Class Permissions
-----------------

Class Permissions are defined on global objects in the Realm file of the special
type `__Class`. Objects of that type have a string primary key (the class name)
and a `LinkList<__Permission>` property in the same way as per-object
permissions.

If a user does not have read access to the `__Class` object, that user will not
be able to see any objects in the class, regardless of whether objects in that
class have ACLs that specifically give that user access. However, having Read
access to a class does NOT mean that the user can see all objects in that class
-- it just means that they can see something at all. They still need
object-level access to individual objects.

In addition to the permissions mentioned under Object Permissions, class
permissions can include the following:

- _Query:_ The user can create partial sync queries based on this class. Queries
  will only include objects which the user can read.
- _Create:_ The user is permitted to create object in this class.
- _ModifySchema:_ The user can add columns to the table.

If the user has the SetPermission privilege for the `__Class` object, the user
can modify the class-level permissions.

**NOTE ABOUT CREATING OBJECTS:** If a user has the `Create` privilege in a
class, but does not have `Update` privileges in that class, they are still able
to modify newly-created objects inside the same transaction as that object was
created.  Any user can create objects of the special type `__Permission` for the
purpose of setting initial permissions on objects they have created.


Realm Permissions
-----------------

Realm Permissions exist in the special table `__Realm`. There is a single global
instance of a `__Realm` object (id=0), which has an ACL representing the
Realm-level permissions. The `__Realm` singleton is created by whoever first
defines a Realm-level permission. Note that Realm-level permissions are
different from the Realm permissions declared by ROS, which go into effect
before the Sync worker is even contacted by the client. This is potential source
of confusion that we well have to fix at some point.

Realm Permissions take precedence over Class Permissions and Object Permissions.
If a user does not have read access to a Realm file, they will not be able to
see anything in that Realm file regardless of class and object permissions
inside that Realm file.

In addition to permissions mentioned under *Class Permissions* and *Object
Permissions*, Realm permissions can include the following:

- _ModifySchema:_ The user can add tables to the Realm file.
- _SetPermissions:_ The user can modify Realm-level permissions in the Realm
  file.

**NOTE:** The long-term goal is to move to in-Realm permissions entirely.
However, in the initial version, in-Realm Realm permissions will coexist with
ROS-provided permissions based on access tokens. This is a potential source of
confusion.


Initial Permissive State
------------------------

When a Realm file is opened on the server through Partial Sync, the sync worker
checks if the special permissions-related tables exist in the Realm file. If
they do not, the sync worker does the following:

- It creates the permissions schema (by calling
  `sync::create_permissions_schema()`).
- It sets up the “permissive defaults”:
    - The role “everyone” is added.
    - Realm-level privileges are defined for “everyone” which lets them modify
      the schema and set permissions.
    - Class-level privileges are defined for “everyone” which lets them create
      new classes.

Note that class-level permissions are not added for user-defined classes. The
initial permissive state only ensures that users have the rights to create and
define classes -- it does not define any class permissions for the user.

For objects, the system assumes that the absence of an ACL column implies full
access to the object (given that the user has access to the class and Realm).
The existence of an empty ACL property, conversely, means that nobody can access
the object. This is to allow the app developer to incrementally add permissions
to their app.


What happens when a user connects?
----------------------------------

- The server checks whether a `__User` object exists with the identity given in
  the access token. If not, the `__User` object is added, and the new user is
  given membership of the “everyone” role.
- If the “everyone” role does not exist, it is added.
- If the user already exists for some reason (an admin may have added them as
  part of an external flow), the worker does NOT add them to the “everyone”
  role.

The client is allowed to create the `__User` object and the “everyone” role if
it wants — the merge logic should resolve any conflicts with the server in the
natural way.


Cheat Sheet
-----------

**FIXME: Insert the cheat sheet from
https://docs.google.com/document/d/1E5W3sQMfWa_wjvG2u43IDsBpeTbhi3g_wrIV9eCKRjc/edit#**


Data Model
----------

    class __Role:
        string name PRIMARY KEY
        LinkList<__User> members

    class __User:
        string id PRIMARY KEY

    class __Permission:
        Link<__Role> role
        bool canRead
        bool canUpdate
        bool canDelete
        bool canSetPermissions
        bool canQuery
        bool canCreate
        bool canModifySchema

    class __Class:
        string name PRIMARY KEY
        LinkList<__Permission> permissions

    class __Realm:
        int id PRIMARY KEY = 0; // singleton
        LinkList<__Permission> permissions



