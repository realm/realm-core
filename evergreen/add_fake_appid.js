// Add a fake version to the schema change history store so that the drop optimization does not run.
// Drop optimization runs asynchronously to drop the schema change history store after PBS sync is
// terminated or a PBS app is deleted and there is only one PBS app present at the time of sync
// termination or app deletion.
// This caused issues with CI tests failing once app deletions starting being done asynchronously
let dummy_app = db.schema_change_history.insertOne({"_id" : new ObjectId()});
if (!dummy_app) {
    throw "Could not insert a dummy app!";
}
