use actix_web::{App, HttpServer};
use actix_web::{
    HttpResponse, Responder, web,
    web::{Data, ServiceConfig},
};
use actixutils::Filters;
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, PgPool, Pool, Postgres};
use std::sync::Arc;
use ferrumec::event::{Event, EventStream, Handler};
use ferrumec::{Infra,Module};
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
    async fn handle(&self, subject: String, message: Vec<u8>) ->Result<(),Box<dyn std::error::Error + Send+Sync>>{
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
        Ok(())
    }
}
#[async_trait]
impl Handler for OnUpdate {
    async fn handle(&self, subject: String, message: Vec<u8>) ->Result<(),Box<dyn std::error::Error + Send+Sync>>{
        match serde_json::from_slice::<Event<UpdateItem>>(&message) {
            Ok(event) => {
                if let Err(e) = self
                    .0
                    .update(&event.payload.id.clone(), event.payload)
                    .await
                {
                    tracing::error!("Failed to update item: {e}");
                }
            }
            Err(e) => {
                tracing::error!("Failed to deserialize event on {}: {}", subject, e);
            }
        }
        Ok(())
    }
}

async fn subscribe(db: DefaultRepo<Item>, es: Arc<dyn EventStream>, namespace: &str) {
    let on_create = Arc::new(OnCreate(db.clone()));
    let on_update = Arc::new(OnUpdate(db.clone()));
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

pub struct CatalogModule {
    pub repo: Data<DefaultRepo<Item>>,
}

impl Module for CatalogModule {
     async fn new(infra: impl Infra) -> Result<Self,Box<dyn std::error::Error>>{
         let target=std::env::var("TARGET").unwrap_or("catalog".into());
         let repo = DefaultRepo::new(infra.database(),infra.cache_factory());
        subscribe(repo.clone(), infra.event_stream(), &target).await;
        Ok(Self {
            repo: Data::new(repo),
        })
    }

     fn configure(self: Arc<Self>, svc: &mut ServiceConfig, namespace: &str) {
        svc.app_data(self.repo.clone())
            .route(namespace, web::get().to(get_items));
    }
}

