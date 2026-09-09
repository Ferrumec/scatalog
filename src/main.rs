use actix_web::{App, HttpServer};
use actix_web::{
    HttpResponse, Responder, web,
    web::{Data, ServiceConfig},
};
use actixutils::Filters;
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, PgPool, Pool, Postgres};
use std::sync::Arc;
use typed_eventbus::{Event, EventStream, Handler, NatsEventStream};
use uuid::Uuid;
use viewset::Repository;
use viewset::{DefaultRepo, Entity};

#[derive(Entity, Serialize, Deserialize, FromRow, Clone)]
#[entity(update = "UpdateItem")]
pub struct Item {
    #[entity(skip_update)]
    pub id: Uuid,
    pub name: String,
    pub slug: Option<String>,
    pub description: Option<String>,
    pub thumbnail: String,
    pub category: Option<String>,
    pub tags: Vec<String>,
}

#[derive(Serialize, Deserialize)]
pub struct UpdateItem {
    pub id: Uuid,
    pub name: Option<String>,
    pub slug: Option<String>,
    pub description: Option<String>,
    pub thumbnail: Option<String>,
    pub category: Option<String>,
    pub tags: Option<Vec<String>>,
}

struct OnCreate(DefaultRepo<Item>);
struct OnUpdate(DefaultRepo<Item>);

use async_trait::async_trait;
#[async_trait]
impl Handler for OnCreate {
    async fn handle(&self, subject: String, message: Vec<u8>) {
        match serde_json::from_slice::<Event<Item>>(&message) {
            Ok(event) => {
                if let Err(e) = self.0.create(event.payload).await {
                    tracing::error!("Failed to create item: {e}");
                }
            }
            Err(e) => {
                tracing::error!("Failed to deserialize event on {}: {}", subject, e);
            }
        }
    }
}
#[async_trait]
impl Handler for OnUpdate {
    async fn handle(&self, subject: String, message: Vec<u8>) {
        match serde_json::from_slice::<Event<UpdateItem>>(&message) {
            Ok(event) => {
                if let Err(e) = self
                    .0
                    .update(&event.payload.id.clone(), event.payload)
                    .await
                {
                    tracing::error!("Failed to create item: {e}");
                }
            }
            Err(e) => {
                tracing::error!("Failed to deserialize event on {}: {}", subject, e);
            }
        }
    }
}

async fn subscribe(db: Pool<Postgres>, es: NatsEventStream, namespace: &str) {
    let on_create = Arc::new(OnCreate(db.clone().into()));
    let on_update = Arc::new(OnUpdate(db.clone().into()));
    let subject = format!("{}.create", namespace);
    if let Err(e) = es.subscribe(subject.clone(), on_create).await {
        tracing::error!(
            "could not subscribe to {subject}: {e}, this is fatal, please fix and restart the service"
        );
    };
    let subject = format!("{}.update", namespace);
    if let Err(e) = es.subscribe(subject.clone(), on_update).await {
        tracing::error!(
            "could not subscribe to {subject}: {e}, this is fatal, please fix and restart the service"
        );
    };
}

async fn get_items(repo: Data<DefaultRepo<Item>>, filters: Filters) -> impl Responder {
    let Ok((items, _)) = repo.list(&filters).await else {
        return HttpResponse::InternalServerError().finish();
    };
    HttpResponse::Ok().json(items)
}

pub struct Module {
    pub repo: Data<DefaultRepo<Item>>,
}

impl Module {
    pub fn new(db: Pool<Postgres>) -> Self {
        Self {
            repo: Data::new(db.into()),
        }
    }

    pub fn config(&self, svc: &mut ServiceConfig, namespace: &str) {
        svc.app_data(self.repo.clone())
            .route(namespace, web::get().to(get_items));
    }
}

use std::env::var;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let db_url = var("DATABASE_URL").expect("var DATABASE_URL not provided");
    let pool = PgPool::connect(&db_url)
        .await
        .expect("could not connect to db");
    let nats_url = var("NATS_URL").expect("var NATS_URL not set");
    let target = var("TARGET").expect("var TARGET not set");
    let nats = NatsEventStream::new(&nats_url)
        .await
        .expect("could not connect to nats");
    subscribe(pool.clone(), nats, &target).await;
    let module = Arc::new(Module::new(pool));

    HttpServer::new(move || App::new().configure(|cfg| module.clone().config(cfg, "items")))
        .bind(("127.0.0.1", 8080))?
        .run()
        .await
}
