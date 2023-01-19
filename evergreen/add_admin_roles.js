let user_doc = db.users.findOne({"data.email": "unique_user@domain.com"});
if (!user_doc) {
    throw "could not find admin user!";
}

let update_res = db.users.updateOne({"_id": user_doc._id}, {
    "$addToSet": {"roles": {"$each": [
        { "roleName": "GLOBAL_STITCH_ADMIN" },
        { "roleName": "GLOBAL_BAAS_FEATURE_ADMIN" }
    ]
}}});

if (update_res.modifiedCount != 1) {
    throw "could not update admin user!";
}
