use actix_web::{get, post, web, App, HttpResponse, HttpServer};
use dotenv::dotenv;
use futures::stream::StreamExt;
use mongodb::{
    bson::{doc, serde_helpers::bson_datetime_as_rfc3339_string},
    options::ClientOptions,
    Client,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::env;

// The amount of readings returned per page.
const PAGE_SIZE: usize = 50;


// A reading from the database.
#[derive(Serialize, Deserialize, Clone)]
struct DBReading {
    reading_at: bson::DateTime,
    data: Vec<ContinuousData>,
    created_at: bson::DateTime,
    patient: Patient,
}

// A format to serialize the incoming JSON payload from the POST request
#[derive(Serialize, Deserialize, Clone)]
struct NewReading {
    #[serde(with = "bson_datetime_as_rfc3339_string")]
    reading_at: bson::DateTime,
    data: Vec<ContinuousData>,
    patient: Patient,
}

// Represents a reading of data from a continuous capture from the HoloLens.
#[derive(Serialize, Deserialize, Clone, Debug)]
struct ContinuousData {
    service_id: String,
    alias: Option<String>,
    value: f32,
    confidence: f32,
}

// Represents the patient data attached to each reading. Supports arbitrary patient data under the data
// key.
#[derive(Serialize, Deserialize, Clone)]
struct Patient {
    bluetooth_id: String,
    alias: Option<String>,
    data: Option<Value>,
}

// An endpoint to see if the API is active.
#[get("/status")]
async fn get_status(_client: web::Data<Client>) -> HttpResponse {
    HttpResponse::Ok().body("Hi")
}

// The optional parameters to the GET /readings request.
#[derive(Serialize, Deserialize)]
struct ReadingsQueryParam {
    // If key exists, API will only return readings from this bluetooth id.
    patient: Option<String>,

    // Specifies a earliest datetime (URL Encoded RFC3339) for the reading data
    from: Option<String>,

    // Specifies a latest datetime (URL Encoded RFC3339) for the reading data
    until: Option<String>,

    // Specifies which page number to return. Readings are returned in blocks of PAGE_SIZE.
    page: Option<u64>,
}

// The format of the JSON respone to the GET /readings request.
#[derive(Serialize, Deserialize)]
struct GetReadingsResponse {
    readings: Vec<DBReading>,
}

// An API endpoint that returns the readings in the database. Can have query paramters:
// patient (bluetooth_id), from, until, page.
#[get("/readings")]
async fn get_readings(
    client: web::Data<Client>,
    query: web::Query<ReadingsQueryParam>,
) -> HttpResponse {
    let readings_collection = client.database("cfa-hud").collection("readings");

    let mut filter_options = bson::Document::new();
    // If patient bluetooth_id specific, adds a filter to the query for only that bluetooth_id.
    if let Some(bid) = &query.patient {
        filter_options.insert("patient.bluetooth_id", bid);
    };

    // Adds filter for readings after the from date. from string must be in URL Encoded RFC3339
    // format.
    if let Some(from) = &query.from {
        println!("{}", from);
        if let Ok(date) = bson::DateTime::parse_rfc3339_str(from) {
            filter_options.insert("reading_at", doc!("$gte": date));
        } else {
            return HttpResponse::BadRequest().body("from date is invalid");
        }
    };

    // Adds filter for readings before the until date. until string must be in URL Encoded RFC3339
    // format.
    if let Some(until) = &query.until {
        println!("{}", until);
        if let Ok(date) = bson::DateTime::parse_rfc3339_str(until) {
            filter_options.insert("reading_at", doc!("$lt": date));
        } else {
            return HttpResponse::BadRequest().body("from date is invalid");
        }
    };

    let find_options_builder = mongodb::options::FindOptions::builder()
        .limit(Some(PAGE_SIZE.try_into().unwrap()))
        .batch_size(Some(PAGE_SIZE.try_into().unwrap()));

    // If page numbers, specified returns readings page * PAGE_SIZE to page + 1 * PAGE_SIZE, if
    // they exist.
    let find_options = match &query.page {
        Some(page) => find_options_builder.skip(page * PAGE_SIZE as u64).build(),
        None => find_options_builder.build(),
    };

    let cursor = readings_collection.find(filter_options, find_options).await;

    match cursor {
        Ok(mut c) => {
            let mut readings = Vec::<DBReading>::new();

            // Iterate through the readings and append them to the results if it exists.
            while let Some(result) = c.next().await {
                match result {
                    Ok(doc) => readings.push(doc),
                    Err(e) => return HttpResponse::InternalServerError().body(e.to_string()),
                }
            }

            // Returns a 200 OK response with the readings
            HttpResponse::Ok().json(web::Json(GetReadingsResponse { readings: readings }))
        }
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

#[derive(Serialize, Deserialize)]
struct PostReadingsPayload {
    readings: Vec<NewReading>,
}

// An API endpoint to add readings to the database. Readings have to be in a specific format given
// by the NewReading struct.
#[post("/readings")]
async fn post_readings(client: web::Data<Client>, payload: web::Json<PostReadingsPayload>) -> HttpResponse {
    let readings_collection = client
        .database("cfa-hud")
        .collection::<DBReading>("readings");

    let new_readings = payload.readings.iter().map(|x| convert_to_db_reading(x)).collect::<Vec<_>>();

    let result = readings_collection.insert_many(new_readings, None).await;

    match result {
        Ok(_) => HttpResponse::Ok().body("200 OK"),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

// Appends the created_at time to the NewReading
fn convert_to_db_reading(reading: &NewReading) -> DBReading {
    let cloned = reading.clone();

    DBReading {
        reading_at: cloned.reading_at,
        data: cloned.data,
        patient: cloned.patient,
        created_at: bson::DateTime::now(),
    }
}

// Connects to the MongoDB database and returns a client handle if successful.
async fn connect_mongodb(connection_string: String) -> mongodb::error::Result<Client> {
    let mut client_options = ClientOptions::parse(connection_string).await?;

    client_options.app_name = Some("CFA HUD".to_string());

    let client = Client::with_options(client_options)?;

    client
        .database("cfa-hud")
        .run_command(doc! {"ping" : 1}, None)
        .await?;

    println!("Connected successfully.");
    Ok(client)
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    dotenv().ok();

    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));

    // Loads the connection string from the environment variables.
    let client = match env::var("CONNECTION_STRING") {
        Err(_) => {
            println!("No connection string");
            None
        }
        Ok(s) => match connect_mongodb(s).await {
            Err(_) => {
                println!("Couldn't complete database connection test.");
                None
            }
            Ok(c) => {
                println!("Completed database connection test.");
                Some(c)
            }
        },
    };

    // Starts the webserver if the app successfully connected to the DB.
    match client {
        None => {
            println!("No client.");
            Ok(())
        }
        Some(c) => {
            HttpServer::new(move || {
                App::new()
                    .wrap(actix_web::middleware::Logger::default())
                    .app_data(web::Data::new(c.clone()))
                    .service(get_status)
                    .service(get_readings)
                    .service(post_readings)
            })
            .bind(("127.0.0.1", 8080))?
            .run()
            .await
        }
    }
}
