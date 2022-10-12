use std::env;
use serde::{Serialize, Deserialize};
use serde_json::{Value, json};
use dotenv::dotenv;
use mongodb::{bson::{doc, serde_helpers::bson_datetime_as_rfc3339_string}, options::ClientOptions, Client};
use futures::stream::StreamExt;
use actix_web::{get, post, web, App, HttpResponse, HttpServer};
use chrono;

const QUERY_LIMIT : i32 = 50;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    dotenv().ok();

    let mut connection_string: Option<String> = None;

    for (key, value) in env::vars() {
        if key == "CONNECTION_STRING" {
            connection_string = Some(value);
        }
    }

    let client : Option<Client>;

    match connection_string {
        None => { println!("No connection string"); client = None; },
        Some(s) =>  { match connect_mongodb(s).await {
            Err(_) => { println!("Couldn't complete database connection test."); client = None;},
            Ok(c) => { client = Some(c); println!("Completed database connection test."); },
        } }
    }

    match client {
        None => { println!("No client."); Ok(()) },
        Some (c) => { 
            HttpServer::new(
                move || {App::new()
                    .app_data(web::Data::new(c.clone()))
                    .service(get_status)
                    .service(get_patient)
                    .service(post_patient)
                    .service(get_readings)
                    .service(post_readings)
                })
                .bind(("127.0.0.1", 8080))?
                .run()
                .await
        }
    }
}


#[get("/status")]
async fn get_status(client: web::Data<Client>) -> HttpResponse {
    HttpResponse::Ok().body("Hi")

}

#[get("/patient/{bid}")]
async fn get_patient(client: web::Data<Client>) -> HttpResponse {
    HttpResponse::Ok().body("Hi")

}

#[post("/patient")]
async fn post_patient(client: web::Data<Client>, patient: web::Json<Patient>) -> HttpResponse {
    HttpResponse::Ok().body("Hi")

}
#[derive(Serialize, Deserialize)]
struct ReadingsQueryParam{
    from: Option<String>,
    until: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct DBReading{
    reading_at: bson::DateTime, 
    data: Vec<ContinuousData>,
    created_at: bson::DateTime,
}



#[derive(Serialize, Deserialize, Clone, Debug)]
struct NewReading{

    #[serde(with = "bson_datetime_as_rfc3339_string")]
    reading_at: bson::DateTime,
    data: Vec<ContinuousData>,
}
#[derive(Serialize, Deserialize)]
struct GetReadingsResponse {
    readings: Vec<DBReading>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct ContinuousData {
    service_id: String,
    alias: String,
    value: f32,
}

#[derive(Serialize, Deserialize)]
struct Patient {
    bid : String,
    alias: String,
    data: Vec<Value>,
}


#[get("/readings")]
async fn get_readings(client: web::Data<Client>, query: web::Query<ReadingsQueryParam>) -> HttpResponse {
    let readings_collection =  client.database("cfa-hud").collection("readings");

    let cursor = readings_collection.find(None, None).await;

    match cursor {
        Err(e) => HttpResponse::InternalServerError().body("Couldn't get readings"),
        Ok(mut c) => {
            let mut readings = Vec::<DBReading>::new();
            while let Some(result) = c.next().await {
                    match result {
                        Ok(doc) => readings.push(doc),
                        _ => return HttpResponse::InternalServerError().body("Couldn't get readings"),
                    }
            }

            HttpResponse::Ok().json(web::Json( GetReadingsResponse {readings: readings}) )
        }
    }
}

#[post("/readings")]
async fn post_readings(client: web::Data<Client>, reading: web::Json<NewReading>) -> HttpResponse {
    let readings_collection =  client.database("cfa-hud").collection::<DBReading>("readings");

    println!("{}", reading.reading_at);
   
    let new_reading = convert_to_db_reading(reading);

    println!("{} {}", new_reading.reading_at, new_reading.created_at);
    let result = readings_collection.insert_one(new_reading, None).await;

    match result {
        Ok(r) => HttpResponse::Ok().body("Hi"),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

fn convert_to_db_reading(reading: web::Json<NewReading>) -> DBReading {
    let cloned = reading.clone();

    DBReading {
        reading_at: cloned.reading_at, 
        data: cloned.data,
        created_at: bson::DateTime::now(),
    }
}

async fn connect_mongodb(connection_string: String) -> mongodb::error::Result<Client> {
    let mut client_options = ClientOptions::parse(connection_string).await?;

    client_options.app_name = Some("CFA HUD".to_string());

    let client = Client::with_options(client_options)?;

    client
        .database("cfa-hud")
        .run_command(doc! {"ping" : 1}, None)
        .await?;

    println!("Connected successfully.");

    for db_name in client.list_database_names(None, None).await? {
        println!("{}", db_name);
    }

    let db = client.database("cfa-hud");

    for collection_name in db.list_collection_names(None).await? {
        println!("{}", collection_name)
    }

    Ok(client)
}
