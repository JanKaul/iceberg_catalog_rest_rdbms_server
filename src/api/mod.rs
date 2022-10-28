use async_trait::async_trait;
use log::info;
use sea_orm::sea_query::OnConflict;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, DatabaseConnection, EntityTrait, ModelTrait, QueryFilter,
};
use serde_json::json;
use swagger::{Has, XSpanIdString};

use iceberg_catalog_rest_rdbms_server::models::{self, TableMetadata};

use iceberg_catalog_rest_rdbms_server::{
    Api, CreateNamespaceResponse, CreateTableResponse, DropNamespaceResponse, DropTableResponse,
    GetConfigResponse, GetTokenResponse, ListNamespacesResponse, ListTablesResponse,
    LoadNamespaceMetadataResponse, LoadTableResponse, RenameTableResponse, ReportMetricsResponse,
    TableExistsResponse, UpdatePropertiesResponse, UpdateTableResponse,
};

use swagger::ApiError;

use crate::database::entities::{prelude::*, *};

#[derive(Clone)]
pub struct Server {
    db: DatabaseConnection,
}

impl Server {
    pub fn new(db: DatabaseConnection) -> Self {
        Server { db }
    }
}

#[async_trait]
impl<C> Api<C> for Server
where
    C: Has<XSpanIdString> + Send + Sync,
{
    /// Create a namespace
    async fn create_namespace(
        &self,
        prefix: String,
        create_namespace_request: Option<models::CreateNamespaceRequest>,
        _context: &C,
    ) -> Result<CreateNamespaceResponse, ApiError> {
        let catalog = if prefix != "" {
            match Catalog::find()
                .filter(catalog::Column::Name.contains(&prefix))
                .one(&self.db)
                .await
                .map_err(|err| ApiError(err.to_string()))?
            {
                None => {
                    let new_catalog = catalog::ActiveModel::from_json(json!({
                        "id": 0,
                        "name": &prefix,
                    }))
                    .map_err(|err| ApiError(err.to_string()))?;

                    let result = Catalog::insert(new_catalog.clone())
                        .on_conflict(
                            // on conflict update
                            OnConflict::column(catalog::Column::Name)
                                .do_nothing()
                                .to_owned(),
                        )
                        .exec(&self.db)
                        .await
                        .map_err(|err| ApiError(err.to_string()))?;

                    Catalog::find_by_id(result.last_insert_id)
                        .one(&self.db)
                        .await
                        .map_err(|err| ApiError(err.to_string()))?
                }
                x => x,
            }
        } else {
            None
        };

        let name = iter_tools::intersperse(
            create_namespace_request
                .ok_or(ApiError("Missing CreateNamespaceRequest.".into()))?
                .namespace
                .into_iter(),
            ".".to_owned(),
        )
        .collect::<String>();

        let new_namespace = namespace::ActiveModel::from_json(json!({
            "id": 0,
            "name": &name,
            "catalog_id": catalog.map(|catalog| catalog.id)
        }))
        .map_err(|err| ApiError(err.to_string()))?;

        Namespace::insert(new_namespace)
            .on_conflict(
                // on conflict update
                OnConflict::column(catalog::Column::Name)
                    .do_nothing()
                    .to_owned(),
            )
            .exec(&self.db)
            .await
            .map_err(|err| ApiError(err.to_string()))?;

        Ok(
            CreateNamespaceResponse::RepresentsASuccessfulCallToCreateANamespace(
                models::CreateNamespace200Response {
                    namespace: vec![name],
                    properties: None,
                },
            ),
        )
    }

    /// Create a table in the given namespace
    async fn create_table(
        &self,
        prefix: String,
        namespace: String,
        create_table_request: Option<models::CreateTableRequest>,
        _context: &C,
    ) -> Result<CreateTableResponse, ApiError> {
        let catalog = if prefix != "" {
            match Catalog::find()
                .filter(catalog::Column::Name.contains(&prefix))
                .one(&self.db)
                .await
                .map_err(|err| ApiError(err.to_string()))?
            {
                None => {
                    let new_catalog = catalog::ActiveModel::from_json(json!({
                        "id": 0,
                        "name": &prefix,
                    }))
                    .map_err(|err| ApiError(err.to_string()))?;

                    let result = Catalog::insert(new_catalog.clone())
                        .on_conflict(
                            // on conflict update
                            OnConflict::column(catalog::Column::Name)
                                .do_nothing()
                                .to_owned(),
                        )
                        .exec(&self.db)
                        .await
                        .map_err(|err| ApiError(err.to_string()))?;

                    Catalog::find_by_id(result.last_insert_id)
                        .one(&self.db)
                        .await
                        .map_err(|err| ApiError(err.to_string()))?
                }
                x => x,
            }
        } else {
            None
        };

        let namespace = match catalog {
            None => Namespace::find()
                .filter(namespace::Column::Name.contains(&namespace))
                .one(&self.db)
                .await
                .map_err(|err| ApiError(err.to_string()))?,
            Some(catalog) => catalog
                .find_related(Namespace)
                .filter(namespace::Column::Name.contains(&namespace))
                .one(&self.db)
                .await
                .map_err(|err| ApiError(err.to_string()))?,
        };

        // Check if namespace exists
        match namespace {
            None => Ok(CreateTableResponse::NotFound(models::ErrorModel::new(
                "The namespace specified does not exist".into(),
                "NotFound".into(),
                500,
            ))),
            Some(namespace) => {
                let name = &create_table_request
                    .as_ref()
                    .ok_or(ApiError("Missing CreateNamespaceRequest.".into()))?
                    .name;
                let metadata_location = &create_table_request
                    .as_ref()
                    .ok_or(ApiError("Missing CreateNamespaceRequest.".into()))?
                    .location
                    .as_ref()
                    .ok_or(ApiError("Missing metadata_location.".into()))?;

                let new_table = iceberg_table::ActiveModel::from_json(json!({
                    "id": 0,
                    "name": &name,
                    "namespace_id": namespace.id,
                    "metadata_location": metadata_location,
                    "previous_metadata_location": None::<String>
                }))
                .map_err(|err| ApiError(err.to_string()))?;

                IcebergTable::insert(new_table)
                    .on_conflict(
                        // on conflict update
                        OnConflict::column(catalog::Column::Name)
                            .do_nothing()
                            .to_owned(),
                    )
                    .exec(&self.db)
                    .await
                    .map_err(|err| ApiError(err.to_string()))?;

                Ok(CreateTableResponse::TableMetadataResultAfterCreatingATable(
                    models::LoadTableResult {
                        metadata_location: Some(metadata_location.to_string()),
                        config: None,
                        metadata: TableMetadata::new(2, "".into()),
                    },
                ))
            }
        }
    }

    /// Drop a namespace from the catalog. Namespace must be empty.
    async fn drop_namespace(
        &self,
        prefix: String,
        namespace: String,
        _context: &C,
    ) -> Result<DropNamespaceResponse, ApiError> {
        let catalog = if prefix != "" {
            Catalog::find()
                .filter(catalog::Column::Name.contains(&prefix))
                .one(&self.db)
                .await
                .map_err(|err| ApiError(err.to_string()))?
        } else {
            None
        };

        let namespace = match catalog {
            None => Namespace::find()
                .filter(namespace::Column::Name.contains(&namespace))
                .one(&self.db)
                .await
                .map_err(|err| ApiError(err.to_string()))?,
            Some(catalog) => catalog
                .find_related(Namespace)
                .filter(namespace::Column::Name.contains(&namespace))
                .one(&self.db)
                .await
                .map_err(|err| ApiError(err.to_string()))?,
        };

        match namespace {
            None => Ok(DropNamespaceResponse::NotFound(models::ErrorModel::new(
                "Namespace to delete does not exist.".into(),
                "NotFound".into(),
                500,
            ))),
            Some(namespace) => {
                namespace
                    .delete(&self.db)
                    .await
                    .map_err(|err| ApiError(err.to_string()))?;

                Ok(DropNamespaceResponse::Success)
            }
        }
    }

    /// Drop a table from the catalog
    async fn drop_table(
        &self,
        prefix: String,
        namespace: String,
        table: String,
        purge_requested: Option<bool>,
        context: &C,
    ) -> Result<DropTableResponse, ApiError> {
        let context = context.clone();
        info!(
            "drop_table(\"{}\", \"{}\", \"{}\", {:?}) - X-Span-ID: {:?}",
            prefix,
            namespace,
            table,
            purge_requested,
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
    }

    /// List namespaces, optionally providing a parent namespace to list underneath
    async fn list_namespaces(
        &self,
        prefix: String,
        parent: Option<String>,
        context: &C,
    ) -> Result<ListNamespacesResponse, ApiError> {
        let context = context.clone();
        info!(
            "list_namespaces(\"{}\", {:?}) - X-Span-ID: {:?}",
            prefix,
            parent,
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
    }

    /// List all table identifiers underneath a given namespace
    async fn list_tables(
        &self,
        prefix: String,
        namespace: String,
        context: &C,
    ) -> Result<ListTablesResponse, ApiError> {
        let context = context.clone();
        info!(
            "list_tables(\"{}\", \"{}\") - X-Span-ID: {:?}",
            prefix,
            namespace,
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
    }

    /// Load the metadata properties for a namespace
    async fn load_namespace_metadata(
        &self,
        prefix: String,
        namespace: String,
        context: &C,
    ) -> Result<LoadNamespaceMetadataResponse, ApiError> {
        let context = context.clone();
        info!(
            "load_namespace_metadata(\"{}\", \"{}\") - X-Span-ID: {:?}",
            prefix,
            namespace,
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
    }

    /// Load a table from the catalog
    async fn load_table(
        &self,
        prefix: String,
        namespace: String,
        table: String,
        context: &C,
    ) -> Result<LoadTableResponse, ApiError> {
        let context = context.clone();
        info!(
            "load_table(\"{}\", \"{}\", \"{}\") - X-Span-ID: {:?}",
            prefix,
            namespace,
            table,
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
    }

    /// Rename a table from its current name to a new name
    async fn rename_table(
        &self,
        prefix: String,
        rename_table_request: models::RenameTableRequest,
        context: &C,
    ) -> Result<RenameTableResponse, ApiError> {
        let context = context.clone();
        info!(
            "rename_table(\"{}\", {:?}) - X-Span-ID: {:?}",
            prefix,
            rename_table_request,
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
    }

    /// Send a metrics report to this endpoint to be processed by the backend
    async fn report_metrics(
        &self,
        prefix: String,
        namespace: String,
        table: String,
        report_metrics_request: models::ReportMetricsRequest,
        context: &C,
    ) -> Result<ReportMetricsResponse, ApiError> {
        let context = context.clone();
        info!(
            "report_metrics(\"{}\", \"{}\", \"{}\", {:?}) - X-Span-ID: {:?}",
            prefix,
            namespace,
            table,
            report_metrics_request,
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
    }

    /// Check if a table exists
    async fn table_exists(
        &self,
        prefix: String,
        namespace: String,
        table: String,
        context: &C,
    ) -> Result<TableExistsResponse, ApiError> {
        let context = context.clone();
        info!(
            "table_exists(\"{}\", \"{}\", \"{}\") - X-Span-ID: {:?}",
            prefix,
            namespace,
            table,
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
    }

    /// Set or remove properties on a namespace
    async fn update_properties(
        &self,
        prefix: String,
        namespace: String,
        update_namespace_properties_request: Option<models::UpdateNamespacePropertiesRequest>,
        context: &C,
    ) -> Result<UpdatePropertiesResponse, ApiError> {
        let context = context.clone();
        info!(
            "update_properties(\"{}\", \"{}\", {:?}) - X-Span-ID: {:?}",
            prefix,
            namespace,
            update_namespace_properties_request,
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
    }

    /// Commit updates to a table
    async fn update_table(
        &self,
        prefix: String,
        namespace: String,
        table: String,
        commit_table_request: Option<models::CommitTableRequest>,
        context: &C,
    ) -> Result<UpdateTableResponse, ApiError> {
        let context = context.clone();
        info!(
            "update_table(\"{}\", \"{}\", \"{}\", {:?}) - X-Span-ID: {:?}",
            prefix,
            namespace,
            table,
            commit_table_request,
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
    }

    /// List all catalog configuration settings
    async fn get_config(&self, context: &C) -> Result<GetConfigResponse, ApiError> {
        let context = context.clone();
        info!("get_config() - X-Span-ID: {:?}", context.get().0.clone());
        Err(ApiError("Generic failure".into()))
    }

    /// Get a token using an OAuth2 flow
    async fn get_token(
        &self,
        grant_type: Option<String>,
        scope: Option<String>,
        client_id: Option<String>,
        client_secret: Option<String>,
        requested_token_type: Option<models::TokenType>,
        subject_token: Option<String>,
        subject_token_type: Option<models::TokenType>,
        actor_token: Option<String>,
        actor_token_type: Option<models::TokenType>,
        context: &C,
    ) -> Result<GetTokenResponse, ApiError> {
        let context = context.clone();
        info!(
            "get_token({:?}, {:?}, {:?}, {:?}, {:?}, {:?}, {:?}, {:?}, {:?}) - X-Span-ID: {:?}",
            grant_type,
            scope,
            client_id,
            client_secret,
            requested_token_type,
            subject_token,
            subject_token_type,
            actor_token,
            actor_token_type,
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
    }
}

#[cfg(test)]

pub mod tests {
    use iceberg_catalog_rest_rdbms_client::{
        apis::{self, configuration::Configuration},
        models::{schema, CreateNamespaceRequest, CreateTableRequest, Schema},
    };

    fn configuration() -> Configuration {
        Configuration {
            base_path: "http://localhost:8080".to_string(),
            user_agent: None,
            client: reqwest::Client::new(),
            basic_auth: None,
            oauth_access_token: None,
            bearer_access_token: None,
            api_key: None,
        }
    }

    #[tokio::test]
    async fn create_namespace() {
        let request = CreateNamespaceRequest {
            namespace: vec!["create_namespace".to_owned()],
            properties: None,
        };
        let response =
            apis::catalog_api_api::create_namespace(&configuration(), "my_catalog", Some(request))
                .await
                .expect("Failed to create namespace");
        assert_eq!(response.namespace[0], "create_namespace");
    }

    #[tokio::test]
    async fn drop_namespace() {
        let request = CreateNamespaceRequest {
            namespace: vec!["drop_namespace".to_owned()],
            properties: None,
        };
        apis::catalog_api_api::create_namespace(&configuration(), "my_catalog", Some(request))
            .await
            .expect("Failed to create namespace");

        apis::catalog_api_api::drop_namespace(&configuration(), "my_catalog", "drop_namespace")
            .await
            .expect("Failed to create namespace");
    }

    #[tokio::test]
    async fn create_table() {
        let namespace_request = CreateNamespaceRequest {
            namespace: vec!["create_table".to_owned()],
            properties: None,
        };
        apis::catalog_api_api::create_namespace(
            &configuration(),
            "my_catalog",
            Some(namespace_request),
        )
        .await
        .expect("Failed to create namespace");

        let mut request = CreateTableRequest::new(
            "create_table".to_owned(),
            Schema::new(schema::RHashType::default(), vec![]),
        );
        request.location = Some("s3://path/to/location".into());
        let response = apis::catalog_api_api::create_table(
            &configuration(),
            "my_catalog",
            "create_table",
            Some(request),
        )
        .await
        .expect("Failed to create table");
        assert_eq!(response.metadata_location.unwrap(), "s3://path/to/location");
    }
}
