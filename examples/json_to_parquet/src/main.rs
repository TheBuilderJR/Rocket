use rocket::{get, post, routes, Rocket, State, serde::json::Json};
use serde::{Serialize, Deserialize};
use serde_json;
use parquet::{file::reader::SerializedFileReader, file::writer::SerializedFileWriter, schema::parser::parse_message_type};
use std::fs::File;
use std::sync::Mutex;

#[derive(Serialize, Deserialize)]
struct JsonData {
    // Define the structure of your JSON data here
}

#[post("/ingest", format = "json", data = "<data>")]
async fn ingest(data: Json<JsonData>, state: &State<Mutex<Vec<JsonData>>>) -> Result<(), String> {
    let mut data_vec = state.lock().expect("lock state");
    data_vec.push(data.into_inner());
    // Here, instead of pushing to a vector, you would write to a parquet file
    Ok(())
}

#[get("/query?<sql_query>")]
async fn query(sql_query: String, state: &State<Mutex<Vec<JsonData>>>) -> Result<Json<Vec<JsonData>>, String> {
    // Here, you would read from a parquet file and execute the SQL query
    let data_vec = state.lock().expect("lock state");
    Ok(Json(data_vec.clone()))
}

#[launch]
fn rocket() -> Rocket {
    rocket::build()
        .manage(Mutex::new(Vec::<JsonData>::new()))
        .mount("/", routes![ingest, query])
}
