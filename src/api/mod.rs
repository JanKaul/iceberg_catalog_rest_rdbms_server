use async_trait::async_trait;
use log::{debug, info};
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
        _purge_requested: Option<bool>,
        _context: &C,
    ) -> Result<DropTableResponse, ApiError> {
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

        let table = match namespace {
            None => IcebergTable::find()
                .filter(iceberg_table::Column::Name.contains(&table))
                .one(&self.db)
                .await
                .map_err(|err| ApiError(err.to_string()))?,
            Some(namespace) => namespace
                .find_related(IcebergTable)
                .filter(iceberg_table::Column::Name.contains(&table))
                .one(&self.db)
                .await
                .map_err(|err| ApiError(err.to_string()))?,
        };
        match table {
            None => Ok(DropTableResponse::NotFound(models::ErrorModel::new(
                "Namespace to delete does not exist.".into(),
                "NotFound".into(),
                500,
            ))),
            Some(table) => {
                table
                    .delete(&self.db)
                    .await
                    .map_err(|err| ApiError(err.to_string()))?;

                Ok(DropTableResponse::Success)
            }
        }
    }

    /// List namespaces, optionally providing a parent namespace to list underneath
    async fn list_namespaces(
        &self,
        prefix: String,
        _parent: Option<String>,
        _context: &C,
    ) -> Result<ListNamespacesResponse, ApiError> {
        let catalog = if prefix != "" {
            Catalog::find()
                .filter(catalog::Column::Name.contains(&prefix))
                .one(&self.db)
                .await
                .map_err(|err| ApiError(err.to_string()))?
        } else {
            None
        };

        let namespaces = match catalog {
            None => Namespace::find()
                .all(&self.db)
                .await
                .map_err(|err| ApiError(err.to_string()))?,
            Some(catalog) => catalog
                .find_related(Namespace)
                .all(&self.db)
                .await
                .map_err(|err| ApiError(err.to_string()))?,
        };

        Ok(ListNamespacesResponse::AListOfNamespaces(
            models::ListNamespaces200Response {
                namespaces: Some(vec![namespaces.into_iter().map(|x| x.name).collect()]),
            },
        ))
    }

    /// List all table identifiers underneath a given namespace
    async fn list_tables(
        &self,
        prefix: String,
        namespace: String,
        _context: &C,
    ) -> Result<ListTablesResponse, ApiError> {
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

        let tables = match namespace.as_ref() {
            None => IcebergTable::find()
                .all(&self.db)
                .await
                .map_err(|err| ApiError(err.to_string()))?,
            Some(namespace) => namespace
                .find_related(IcebergTable)
                .all(&self.db)
                .await
                .map_err(|err| ApiError(err.to_string()))?,
        };
        Ok(ListTablesResponse::AListOfTableIdentifiers(
            models::ListTables200Response {
                identifiers: Some(
                    tables
                        .into_iter()
                        .map(|x| models::TableIdentifier {
                            name: x.name,
                            namespace: match namespace.as_ref() {
                                Some(namespace) => {
                                    namespace.name.split('.').map(|s| s.to_owned()).collect()
                                }
                                None => vec![],
                            },
                        })
                        .collect(),
                ),
            },
        ))
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
        _context: &C,
    ) -> Result<LoadTableResponse, ApiError> {
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

        let table = match namespace {
            None => IcebergTable::find()
                .filter(iceberg_table::Column::Name.contains(&table))
                .one(&self.db)
                .await
                .map_err(|err| ApiError(err.to_string()))?,
            Some(namespace) => namespace
                .find_related(IcebergTable)
                .filter(iceberg_table::Column::Name.contains(&table))
                .one(&self.db)
                .await
                .map_err(|err| ApiError(err.to_string()))?,
        };
        match table {
            None => Ok(LoadTableResponse::NotFound(models::ErrorModel::new(
                "Namespace to delete does not exist.".into(),
                "NotFound".into(),
                500,
            ))),
            Some(table) => Ok(LoadTableResponse::TableMetadataResultWhenLoadingATable(
                models::LoadTableResult {
                    metadata_location: Some(table.metadata_location.to_string()),
                    config: None,
                    metadata: TableMetadata::new(2, "".into()),
                },
            )),
        }
    }

    /// Rename a table from its current name to a new name
    async fn rename_table(
        &self,
        prefix: String,
        rename_table_request: models::RenameTableRequest,
        _context: &C,
    ) -> Result<RenameTableResponse, ApiError> {
        let old_namespace_name = iter_tools::intersperse(
            rename_table_request.source.namespace.into_iter(),
            ".".into(),
        )
        .collect::<String>();
        let old_name = rename_table_request.source.name;
        let new_namespace_name = iter_tools::intersperse(
            rename_table_request.destination.namespace.into_iter(),
            ".".into(),
        )
        .collect::<String>();
        let new_name = rename_table_request.destination.name;
        let catalog = if prefix != "" {
            Catalog::find()
                .filter(catalog::Column::Name.contains(&prefix))
                .one(&self.db)
                .await
                .map_err(|err| ApiError(err.to_string()))?
        } else {
            None
        };

        let old_namespace = match &catalog {
            None => Namespace::find()
                .filter(namespace::Column::Name.contains(&old_namespace_name))
                .one(&self.db)
                .await
                .map_err(|err| ApiError(err.to_string()))?,
            Some(catalog) => catalog
                .find_related(Namespace)
                .filter(namespace::Column::Name.contains(&old_namespace_name))
                .one(&self.db)
                .await
                .map_err(|err| ApiError(err.to_string()))?,
        };

        let new_namespace = if old_namespace_name == new_namespace_name {
            old_namespace.clone()
        } else {
            match &catalog {
                None => Namespace::find()
                    .filter(namespace::Column::Name.contains(&new_namespace_name))
                    .one(&self.db)
                    .await
                    .map_err(|err| ApiError(err.to_string()))?,
                Some(catalog) => catalog
                    .find_related(Namespace)
                    .filter(namespace::Column::Name.contains(&new_namespace_name))
                    .one(&self.db)
                    .await
                    .map_err(|err| ApiError(err.to_string()))?,
            }
        };

        let table = match old_namespace {
            None => IcebergTable::find()
                .filter(iceberg_table::Column::Name.contains(&old_name))
                .one(&self.db)
                .await
                .map_err(|err| ApiError(err.to_string()))?,
            Some(old_namespace) => old_namespace
                .find_related(IcebergTable)
                .filter(iceberg_table::Column::Name.contains(&old_name))
                .one(&self.db)
                .await
                .map_err(|err| ApiError(err.to_string()))?,
        };
        match (table, new_namespace) {
            (None, _) => Ok(RenameTableResponse::NotFound(models::ErrorModel::new(
                "Table to rename does not exist.".into(),
                "Not Found - NoSuchTableException".into(),
                500,
            ))),
            (_, None) => Ok(RenameTableResponse::NotFound(models::ErrorModel::new(
                "The target namespace of the new table identifier does not exist.".into(),
                "NoSuchNamespaceException".into(),
                500,
            ))),
            (Some(table), Some(namespace)) => {
                let mut new_table: iceberg_table::ActiveModel = table.into();
                new_table.set(iceberg_table::Column::Name, new_name.into());
                new_table.set(iceberg_table::Column::NamespaceId, namespace.id.into());
                new_table.update(&self.db).await.map_err(|err| {
                    debug!("{}", &err);
                    ApiError(err.to_string())
                })?;
                Ok(RenameTableResponse::OK)
            }
        }
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
        _context: &C,
    ) -> Result<TableExistsResponse, ApiError> {
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

        let table = match namespace {
            None => IcebergTable::find()
                .filter(iceberg_table::Column::Name.contains(&table))
                .one(&self.db)
                .await
                .map_err(|err| ApiError(err.to_string()))?,
            Some(namespace) => namespace
                .find_related(IcebergTable)
                .filter(iceberg_table::Column::Name.contains(&table))
                .one(&self.db)
                .await
                .map_err(|err| ApiError(err.to_string()))?,
        };
        match table {
            None => Ok(TableExistsResponse::NotFound),
            Some(_) => Ok(TableExistsResponse::OK),
        }
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
        _context: &C,
    ) -> Result<UpdateTableResponse, ApiError> {
        match commit_table_request {
            None => Ok(UpdateTableResponse::IndicatesABadRequestError(
                models::ErrorModel::new("No CommitTableRequest".into(), "BadRequest".into(), 500),
            )),
            Some(commit_table_request) => {
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

                let table = match namespace {
                    None => IcebergTable::find()
                        .filter(iceberg_table::Column::Name.contains(&table))
                        .one(&self.db)
                        .await
                        .map_err(|err| ApiError(err.to_string()))?,
                    Some(namespace) => namespace
                        .find_related(IcebergTable)
                        .filter(iceberg_table::Column::Name.contains(&table))
                        .one(&self.db)
                        .await
                        .map_err(|err| ApiError(err.to_string()))?,
                };
                match table {
                    None => Ok(UpdateTableResponse::NotFound(models::ErrorModel::new(
                        "Namespace to delete does not exist.".into(),
                        "NotFound".into(),
                        500,
                    ))),
                    Some(table) => {
                        let old_metadata_location = table.metadata_location.clone();
                        let mut new_table: iceberg_table::ActiveModel = table.into();
                        new_table.set(
                            iceberg_table::Column::MetadataLocation,
                            commit_table_request
                                .updates
                                .iter()
                                .last()
                                .unwrap()
                                .location
                                .as_str()
                                .into(),
                        );
                        new_table.set(
                            iceberg_table::Column::PreviousMetadataLocation,
                            Some(old_metadata_location).into(),
                        );
                        let new_table = new_table.update(&self.db).await.map_err(|err| {
                            debug!("{}", &err);
                            ApiError(err.to_string())
                        })?;
                        Ok(
                            UpdateTableResponse::ResponseUsedWhenATableIsSuccessfullyUpdated(
                                models::UpdateTable200Response {
                                    metadata_location: new_table.metadata_location,
                                    metadata: TableMetadata::new(2, "".into()),
                                },
                            ),
                        )
                    }
                }
            }
        }
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
    use std::collections::HashMap;

    use iceberg_catalog_rest_rdbms_client::{
        apis::{self, configuration::Configuration},
        models::{self, schema, Schema},
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
        let request = models::CreateNamespaceRequest {
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
        let request = models::CreateNamespaceRequest {
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
        let namespace_request = models::CreateNamespaceRequest {
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

        let mut request = models::CreateTableRequest::new(
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

    #[tokio::test]
    async fn drop_table() {
        let namespace_request = models::CreateNamespaceRequest {
            namespace: vec!["drop_table".to_owned()],
            properties: None,
        };
        apis::catalog_api_api::create_namespace(
            &configuration(),
            "my_catalog",
            Some(namespace_request),
        )
        .await
        .expect("Failed to create namespace");

        let mut request = models::CreateTableRequest::new(
            "drop_table".to_owned(),
            Schema::new(schema::RHashType::default(), vec![]),
        );
        request.location = Some("s3://path/to/location".into());
        apis::catalog_api_api::create_table(
            &configuration(),
            "my_catalog",
            "drop_table",
            Some(request),
        )
        .await
        .expect("Failed to create table");

        apis::catalog_api_api::drop_table(
            &configuration(),
            "my_catalog",
            "drop_table",
            "drop_table",
            Some(true),
        )
        .await
        .expect("Failed to create namespace");
    }

    #[tokio::test]
    async fn list_namespaces() {
        let request1 = models::CreateNamespaceRequest {
            namespace: vec!["list_namespaces1".to_owned()],
            properties: None,
        };
        apis::catalog_api_api::create_namespace(&configuration(), "my_catalog", Some(request1))
            .await
            .expect("Failed to create namespace");

        let request2 = models::CreateNamespaceRequest {
            namespace: vec!["list_namespaces2".to_owned()],
            properties: None,
        };
        apis::catalog_api_api::create_namespace(&configuration(), "my_catalog", Some(request2))
            .await
            .expect("Failed to create namespace");

        let response = apis::catalog_api_api::list_namespaces(&configuration(), "my_catalog", None)
            .await
            .expect("Failed to list namespace");
        assert_eq!(
            response.namespaces.unwrap()[0],
            vec!["list_namespaces1", "list_namespaces2"]
        );
    }

    #[tokio::test]
    async fn list_tables() {
        let namespace_request1 = models::CreateNamespaceRequest {
            namespace: vec!["list_tables".to_owned()],
            properties: None,
        };
        apis::catalog_api_api::create_namespace(
            &configuration(),
            "my_catalog",
            Some(namespace_request1),
        )
        .await
        .expect("Failed to create namespace");

        let mut request1 = models::CreateTableRequest::new(
            "list_tables1".to_owned(),
            Schema::new(schema::RHashType::default(), vec![]),
        );
        request1.location = Some("s3://path/to/location".into());
        apis::catalog_api_api::create_table(
            &configuration(),
            "my_catalog",
            "list_tables",
            Some(request1),
        )
        .await
        .expect("Failed to create table");
        let mut request2 = models::CreateTableRequest::new(
            "list_tables2".to_owned(),
            Schema::new(schema::RHashType::default(), vec![]),
        );
        request2.location = Some("s3://path/to/location".into());
        apis::catalog_api_api::create_table(
            &configuration(),
            "my_catalog",
            "list_tables",
            Some(request2),
        )
        .await
        .expect("Failed to create table");
        let response =
            apis::catalog_api_api::list_tables(&configuration(), "my_catalog", "list_tables")
                .await
                .expect("Failed to create table");
        assert_eq!(
            response
                .identifiers
                .unwrap()
                .into_iter()
                .map(|x| x.name)
                .collect::<Vec<_>>(),
            vec!["list_tables1", "list_tables2"]
        );
    }

    #[tokio::test]
    async fn load_table() {
        let namespace_request = models::CreateNamespaceRequest {
            namespace: vec!["load_table".to_owned()],
            properties: None,
        };
        apis::catalog_api_api::create_namespace(
            &configuration(),
            "my_catalog",
            Some(namespace_request),
        )
        .await
        .expect("Failed to create namespace");

        let mut request = models::CreateTableRequest::new(
            "load_table".to_owned(),
            Schema::new(schema::RHashType::default(), vec![]),
        );
        request.location = Some("s3://path/to/location".into());
        apis::catalog_api_api::create_table(
            &configuration(),
            "my_catalog",
            "load_table",
            Some(request),
        )
        .await
        .expect("Failed to create table");

        let response = apis::catalog_api_api::load_table(
            &configuration(),
            "my_catalog",
            "load_table",
            "load_table",
        )
        .await
        .expect("Failed to create namespace");

        assert_eq!(response.metadata_location.unwrap(), "s3://path/to/location")
    }

    #[tokio::test]
    async fn update_table() {
        let namespace_request = models::CreateNamespaceRequest {
            namespace: vec!["update_table".to_owned()],
            properties: None,
        };
        apis::catalog_api_api::create_namespace(
            &configuration(),
            "my_catalog",
            Some(namespace_request),
        )
        .await
        .expect("Failed to create namespace");

        let mut create_request = models::CreateTableRequest::new(
            "update_table".to_owned(),
            Schema::new(schema::RHashType::default(), vec![]),
        );
        create_request.location = Some("s3://path/to/location1".into());
        let create_response = apis::catalog_api_api::create_table(
            &configuration(),
            "my_catalog",
            "update_table",
            Some(create_request),
        )
        .await
        .expect("Failed to create table");

        let request = models::CommitTableRequest::new(
            vec![],
            vec![models::TableUpdate::new(
                models::table_update::Action::SetLocation,
                create_response.metadata.format_version,
                create_response
                    .metadata
                    .schemas
                    .map(|x| x[0].clone())
                    .unwrap_or_else(|| models::Schema::new(schema::RHashType::default(), vec![])),
                create_response.metadata.current_schema_id.unwrap_or(0),
                create_response
                    .metadata
                    .partition_specs
                    .map(|x| x[0].clone())
                    .unwrap_or_else(|| models::PartitionSpec::default()),
                create_response.metadata.default_spec_id.unwrap_or(0),
                create_response
                    .metadata
                    .sort_orders
                    .map(|x| x[0].clone())
                    .unwrap_or_else(|| models::SortOrder::default()),
                create_response.metadata.default_sort_order_id.unwrap_or(0),
                create_response
                    .metadata
                    .snapshots
                    .map(|x| x[0].clone())
                    .unwrap_or_else(|| models::Snapshot::default()),
                models::table_update::RHashType::default(),
                create_response.metadata.current_snapshot_id.unwrap_or(0) as i64,
                "update_table".into(),
                vec![],
                "s3://path/to/location2".into(),
                HashMap::new(),
                vec![],
            )],
        );
        let response = apis::catalog_api_api::update_table(
            &configuration(),
            "my_catalog",
            "update_table",
            "update_table",
            Some(request),
        )
        .await
        .expect("Failed to create table");

        assert_eq!(response.metadata_location, "s3://path/to/location2");
    }

    #[tokio::test]
    async fn rename_table() {
        let namespace_request = models::CreateNamespaceRequest {
            namespace: vec!["rename_table".to_owned()],
            properties: None,
        };
        apis::catalog_api_api::create_namespace(
            &configuration(),
            "my_catalog",
            Some(namespace_request),
        )
        .await
        .expect("Failed to create namespace");

        let mut create_request = models::CreateTableRequest::new(
            "rename_table1".to_owned(),
            Schema::new(schema::RHashType::default(), vec![]),
        );
        create_request.location = Some("s3://path/to/location1".into());
        apis::catalog_api_api::create_table(
            &configuration(),
            "my_catalog",
            "rename_table",
            Some(create_request),
        )
        .await
        .expect("Failed to create table");

        let request = models::RenameTableRequest::new(
            models::TableIdentifier {
                name: "rename_table1".into(),
                namespace: vec!["rename_table".into()],
            },
            models::TableIdentifier {
                name: "rename_table2".into(),
                namespace: vec!["rename_table".into()],
            },
        );
        apis::catalog_api_api::rename_table(&configuration(), "my_catalog", request)
            .await
            .expect("Failed to create table");

        apis::catalog_api_api::table_exists(
            &configuration(),
            "my_catalog",
            "rename_table",
            "rename_table2",
        )
        .await
        .expect("Failed to create table");
    }

    #[tokio::test]
    async fn table_exists() {
        let namespace_request = models::CreateNamespaceRequest {
            namespace: vec!["table_exists".to_owned()],
            properties: None,
        };
        apis::catalog_api_api::create_namespace(
            &configuration(),
            "my_catalog",
            Some(namespace_request),
        )
        .await
        .expect("Failed to create namespace");

        let mut create_request = models::CreateTableRequest::new(
            "table_exists1".to_owned(),
            Schema::new(schema::RHashType::default(), vec![]),
        );
        create_request.location = Some("s3://path/to/location1".into());
        apis::catalog_api_api::create_table(
            &configuration(),
            "my_catalog",
            "table_exists",
            Some(create_request),
        )
        .await
        .expect("Failed to create table");

        apis::catalog_api_api::table_exists(
            &configuration(),
            "my_catalog",
            "table_exists",
            "table_exists1",
        )
        .await
        .expect("Failed to create table");
    }
}
