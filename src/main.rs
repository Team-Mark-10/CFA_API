use dotenv::dotenv;
use mongodb::{bson::doc, options::ClientOptions, Client};
use std::env;

#[tokio::main]
async fn main() {
    dotenv().ok();

    let mut connection_string: Option<String> = None;

    for (key, value) in env::vars() {
        if key == "CONNECTION_STRING" {
            connection_string = Some(value);
        }
    }

    match connection_string {
        None => { println!("No connection string"); return (); },
        Some(s) =>  { connect_mongodb(s).await; return (); }
    }
}

async fn connect_mongodb(connection_string: String) -> mongodb::error::Result<()> {
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

    Ok(())
}
