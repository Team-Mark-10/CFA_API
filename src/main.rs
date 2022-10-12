use std::env;
use dotenv::dotenv;
use mongodb::{bson::doc, options::ClientOptions, Client};
use actix_web::{get, post, web, App, HttpResponse, HttpServer};

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
                    .service(ping)
                })
                .bind(("127.0.0.1", 8080))?
                .run()
                .await
        }
    }
}


#[get("/status")]
async fn ping(client: web::Data<Client>) -> HttpResponse {
    HttpResponse::Ok().body("Hi")

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
