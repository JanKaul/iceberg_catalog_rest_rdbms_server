use async_trait::async_trait;
use log::info;
use std::marker::PhantomData;
use swagger::{Has, XSpanIdString};

use iceberg_catalog_rest_rdbms_server::models;

use iceberg_catalog_rest_rdbms_server::{
    Api, CreateNamespaceResponse, CreateTableResponse, DropNamespaceResponse, DropTableResponse,
    GetConfigResponse, GetTokenResponse, ListNamespacesResponse, ListTablesResponse,
    LoadNamespaceMetadataResponse, LoadTableResponse, RenameTableResponse, ReportMetricsResponse,
    TableExistsResponse, UpdatePropertiesResponse, UpdateTableResponse,
};
use swagger::ApiError;

#[derive(Copy, Clone)]
pub struct Server<C> {
    marker: PhantomData<C>,
}

impl<C> Server<C> {
    pub fn new() -> Self {
        Server {
            marker: PhantomData,
        }
    }
}

#[async_trait]
impl<C> Api<C> for Server<C>
where
    C: Has<XSpanIdString> + Send + Sync,
{
    /// Create a namespace
    async fn create_namespace(
        &self,
        prefix: String,
        create_namespace_request: Option<models::CreateNamespaceRequest>,
        context: &C,
    ) -> Result<CreateNamespaceResponse, ApiError> {
        let context = context.clone();
        info!(
            "create_namespace(\"{}\", {:?}) - X-Span-ID: {:?}",
            prefix,
            create_namespace_request,
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
    }

    /// Create a table in the given namespace
    async fn create_table(
        &self,
        prefix: String,
        namespace: String,
        create_table_request: Option<models::CreateTableRequest>,
        context: &C,
    ) -> Result<CreateTableResponse, ApiError> {
        let context = context.clone();
        info!(
            "create_table(\"{}\", \"{}\", {:?}) - X-Span-ID: {:?}",
            prefix,
            namespace,
            create_table_request,
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
    }

    /// Drop a namespace from the catalog. Namespace must be empty.
    async fn drop_namespace(
        &self,
        prefix: String,
        namespace: String,
        context: &C,
    ) -> Result<DropNamespaceResponse, ApiError> {
        let context = context.clone();
        info!(
            "drop_namespace(\"{}\", \"{}\") - X-Span-ID: {:?}",
            prefix,
            namespace,
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
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
