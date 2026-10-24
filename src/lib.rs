use actix_web::{web, web::ServiceConfig};
use ferrumec::event::{Event, EventStream, Handler};
use ferrumec::{Infra, Module};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use std::sync::Arc;
use uuid::Uuid;
use viewset::{
    DefaultRepo, Entity, Repository, ViewSet,
    web::{DefaultService, SessionMiddleware},
};

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
    async fn handle(
        &self,
        subject: String,
        message: Vec<u8>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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
    async fn handle(
        &self,
        subject: String,
        message: Vec<u8>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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

type ItemsService = DefaultService<DefaultRepo<Item>>;

struct MainViewSet {
    svc: ItemsService,
    middleware: Arc<SessionMiddleware>,
}

impl MainViewSet {
    fn new(infra: impl Infra) -> Self {
        let repo = DefaultRepo::new(infra.database(), infra.cache_factory());
        let svc: ItemsService = repo.into();
        let middleware = Arc::new(SessionMiddleware::new(infra.cache_factory()));
        Self { svc, middleware }
    }
}

impl ViewSet for MainViewSet {
    type Service = ItemsService;

    fn service(&self) -> &ItemsService {
        &self.svc
    }

    fn configure(self: Arc<Self>, cfg: &mut web::ServiceConfig, path: &str)
    where
        Self: Sized,
    {
        let vs_list = self.clone();
        let vs_get = self.clone();
        let vs_post = self.clone();
        let vs_put = self.clone();
        let vs_patch = self.clone();
        let vs_delete = self.clone();

        // Read-only endpoints: no session middleware.
        cfg.service(
            web::resource(path)
                .route(web::get().to(move |f| Self::handle_list(vs_list.clone(), f))),
        )
        .service(
            web::resource(format!("{}/{{id}}", path))
                .route(web::get().to(move |id| Self::handle_retrieve(vs_get.clone(), id))),
        );

        // Modifying endpoints: isolate these in a separate scope.
        cfg.service(
            web::scope(path)
                // Attach the session middleware here.
                .wrap(self.middleware.clone())
                .route(
                    "",
                    web::post().to(move |body| Self::handle_create(vs_post.clone(), body)),
                )
                .route(
                    "/{id}",
                    web::put().to(move |id, body| Self::handle_update(vs_put.clone(), id, body)),
                )
                .route(
                    "/{id}",
                    web::patch()
                        .to(move |id, body| Self::handle_update(vs_patch.clone(), id, body)),
                )
                .route(
                    "/{id}",
                    web::delete().to(move |id| Self::handle_delete(vs_delete.clone(), id)),
                ),
        );
    }
}

pub struct CatalogModule {
    viewset: Arc<MainViewSet>,
}

impl Module for CatalogModule {
    async fn new(infra: impl Infra) -> Result<Self, Box<dyn std::error::Error>> {
        let target = std::env::var("TARGET").unwrap_or("catalog".into());
        let repo = DefaultRepo::new(infra.database(), infra.cache_factory());
        subscribe(repo.clone(), infra.event_stream(), &target).await;
        let viewset = Arc::new(MainViewSet::new(infra));
        Ok(Self { viewset })
    }

    fn configure(self: Arc<Self>, svc: &mut ServiceConfig, namespace: &str) {
        svc.configure(|cfg| self.viewset.clone().configure(cfg, namespace));
    }
}
