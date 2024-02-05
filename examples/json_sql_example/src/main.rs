#[macro_use] extern crate rocket;
#[macro_use] extern crate serde_derive;
extern crate serde;
extern crate sqlx;

use rocket::serde::{json::Json, Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Pool, Postgres};

#[derive(Serialize, Deserialize, sqlx::FromRow)]
struct Item {
    id: i32,
    name: String,
    description: String,
}

#[post("/submit", format = "json", data = "<item>")]
async fn submit(pool: &rocket::State<Pool<Postgres>>, item: Json<Item>) -> Result<(), rocket::http::Status> {
    sqlx::query!("INSERT INTO items (name, description) VALUES ($1, $2)", item.name, item.description)
        .execute(pool.inner().deref())
        .await
        .map_err(|_| rocket::http::Status::InternalServerError)?;
    Ok(())
}

#[get("/query")]
async fn query(pool: &rocket::State<Pool<Postgres>>) -> Result<Json<Vec<Item>>, rocket::http::Status> {
    let items = sqlx::query_as!(Item, "SELECT id, name, description FROM items")
        .fetch_all(pool.inner().deref())
        .await
        .map_err(|_| rocket::http::Status::InternalServerError)?;
    Ok(Json(items))
}

#[rocket::main]
async fn main() {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect("postgres://user:password@localhost/database")
        .await
        .expect("Failed to create pool.");

    let _ = rocket::build()
        .manage(pool)
        .mount("/", routes![submit, query])
        .launch()
        .await;
}
