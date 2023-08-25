// Add a version to the schema change history store so that the drop optimization does not take place
// This caused issues with this test failing once app deletions starting being done asynchronously
let dummy_app = db.schema_change_history.insertOne({ "_id": new ObjectId() });
if (!dummy_app) {
    throw "Could not insert a dummy app!";
}
