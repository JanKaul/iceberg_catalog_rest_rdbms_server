# Rust API for openapi_client

Defines the specification for the first version of the REST Catalog API. Implementations should ideally support both Iceberg table specs v1 and v2, with priority given to v2.

## Overview

This client/server was generated by the [openapi-generator]
(https://openapi-generator.tech) project.  By using the
[OpenAPI-Spec](https://github.com/OAI/OpenAPI-Specification) from a remote
server, you can easily generate a server stub.

To see how to make this your own, look here:

[README]((https://openapi-generator.tech))

- API version: 0.0.1
- Build date: 2022-10-26T10:05:47.803+02:00[Europe/Berlin]



This autogenerated project defines an API crate `openapi_client` which contains:
* An `Api` trait defining the API in Rust.
* Data types representing the underlying data model.
* A `Client` type which implements `Api` and issues HTTP requests for each operation.
* A router which accepts HTTP requests and invokes the appropriate `Api` method for each operation.

It also contains an example server and client which make use of `openapi_client`:

* The example server starts up a web server using the `openapi_client`
    router, and supplies a trivial implementation of `Api` which returns failure
    for every operation.
* The example client provides a CLI which lets you invoke
    any single operation on the `openapi_client` client by passing appropriate
    arguments on the command line.

You can use the example server and client as a basis for your own code.
See below for [more detail on implementing a server](#writing-a-server).

## Examples

Run examples with:

```
cargo run --example <example-name>
```

To pass in arguments to the examples, put them after `--`, for example:

```
cargo run --example client -- --help
```

### Running the example server
To run the server, follow these simple steps:

```
cargo run --example server
```

### Running the example client
To run a client, follow one of the following simple steps:

```
cargo run --example client CreateNamespace
cargo run --example client CreateTable
cargo run --example client DropNamespace
cargo run --example client DropTable
cargo run --example client ListNamespaces
cargo run --example client ListTables
cargo run --example client LoadNamespaceMetadata
cargo run --example client LoadTable
cargo run --example client TableExists
cargo run --example client UpdateProperties
cargo run --example client UpdateTable
cargo run --example client GetConfig
cargo run --example client GetToken
```

### HTTPS
The examples can be run in HTTPS mode by passing in the flag `--https`, for example:

```
cargo run --example server -- --https
```

This will use the keys/certificates from the examples directory. Note that the
server chain is signed with `CN=localhost`.

## Using the generated library

The generated library has a few optional features that can be activated through Cargo.

* `server`
    * This defaults to enabled and creates the basic skeleton of a server implementation based on hyper
    * To create the server stack you'll need to provide an implementation of the API trait to provide the server function.
* `client`
    * This defaults to enabled and creates the basic skeleton of a client implementation based on hyper
    * The constructed client implements the API trait by making remote API call.
* `conversions`
    * This defaults to disabled and creates extra derives on models to allow "transmogrification" between objects of structurally similar types.

See https://doc.rust-lang.org/cargo/reference/manifest.html#the-features-section for how to use features in your `Cargo.toml`.

## Documentation for API Endpoints

All URIs are relative to *https://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**createNamespace**](docs/catalog_api_api.md#createNamespace) | **POST** /v1/{prefix}/namespaces | Create a namespace
[**createTable**](docs/catalog_api_api.md#createTable) | **POST** /v1/{prefix}/namespaces/{namespace}/tables | Create a table in the given namespace
[**dropNamespace**](docs/catalog_api_api.md#dropNamespace) | **DELETE** /v1/{prefix}/namespaces/{namespace} | Drop a namespace from the catalog. Namespace must be empty.
[**dropTable**](docs/catalog_api_api.md#dropTable) | **DELETE** /v1/{prefix}/namespaces/{namespace}/tables/{table} | Drop a table from the catalog
[**listNamespaces**](docs/catalog_api_api.md#listNamespaces) | **GET** /v1/{prefix}/namespaces | List namespaces, optionally providing a parent namespace to list underneath
[**listTables**](docs/catalog_api_api.md#listTables) | **GET** /v1/{prefix}/namespaces/{namespace}/tables | List all table identifiers underneath a given namespace
[**loadNamespaceMetadata**](docs/catalog_api_api.md#loadNamespaceMetadata) | **GET** /v1/{prefix}/namespaces/{namespace} | Load the metadata properties for a namespace
[**loadTable**](docs/catalog_api_api.md#loadTable) | **GET** /v1/{prefix}/namespaces/{namespace}/tables/{table} | Load a table from the catalog
[**renameTable**](docs/catalog_api_api.md#renameTable) | **POST** /v1/{prefix}/tables/rename | Rename a table from its current name to a new name
[**reportMetrics**](docs/catalog_api_api.md#reportMetrics) | **POST** /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics | Send a metrics report to this endpoint to be processed by the backend
[**tableExists**](docs/catalog_api_api.md#tableExists) | **HEAD** /v1/{prefix}/namespaces/{namespace}/tables/{table} | Check if a table exists
[**updateProperties**](docs/catalog_api_api.md#updateProperties) | **POST** /v1/{prefix}/namespaces/{namespace}/properties | Set or remove properties on a namespace
[**updateTable**](docs/catalog_api_api.md#updateTable) | **POST** /v1/{prefix}/namespaces/{namespace}/tables/{table} | Commit updates to a table
[**getConfig**](docs/configuration_api_api.md#getConfig) | **GET** /v1/config | List all catalog configuration settings
[**getToken**](docs/o_auth2_api_api.md#getToken) | **POST** /v1/oauth/tokens | Get a token using an OAuth2 flow


## Documentation For Models

 - [AddPartitionSpecUpdate](docs/AddPartitionSpecUpdate.md)
 - [AddPartitionSpecUpdateAllOf](docs/AddPartitionSpecUpdateAllOf.md)
 - [AddSchemaUpdate](docs/AddSchemaUpdate.md)
 - [AddSchemaUpdateAllOf](docs/AddSchemaUpdateAllOf.md)
 - [AddSnapshotUpdate](docs/AddSnapshotUpdate.md)
 - [AddSnapshotUpdateAllOf](docs/AddSnapshotUpdateAllOf.md)
 - [AddSortOrderUpdate](docs/AddSortOrderUpdate.md)
 - [AddSortOrderUpdateAllOf](docs/AddSortOrderUpdateAllOf.md)
 - [AndOrExpression](docs/AndOrExpression.md)
 - [BaseUpdate](docs/BaseUpdate.md)
 - [CatalogConfig](docs/CatalogConfig.md)
 - [CommitTableRequest](docs/CommitTableRequest.md)
 - [CounterResult](docs/CounterResult.md)
 - [CreateNamespace200Response](docs/CreateNamespace200Response.md)
 - [CreateNamespaceRequest](docs/CreateNamespaceRequest.md)
 - [CreateTableRequest](docs/CreateTableRequest.md)
 - [ErrorModel](docs/ErrorModel.md)
 - [Expression](docs/Expression.md)
 - [ExpressionType](docs/ExpressionType.md)
 - [GetToken200Response](docs/GetToken200Response.md)
 - [GetToken400Response](docs/GetToken400Response.md)
 - [ListNamespaces200Response](docs/ListNamespaces200Response.md)
 - [ListTables200Response](docs/ListTables200Response.md)
 - [ListType](docs/ListType.md)
 - [LiteralExpression](docs/LiteralExpression.md)
 - [LoadNamespaceMetadata200Response](docs/LoadNamespaceMetadata200Response.md)
 - [LoadTableResult](docs/LoadTableResult.md)
 - [MapType](docs/MapType.md)
 - [MetadataLogInner](docs/MetadataLogInner.md)
 - [MetricResult](docs/MetricResult.md)
 - [NotExpression](docs/NotExpression.md)
 - [NullOrder](docs/NullOrder.md)
 - [PartitionField](docs/PartitionField.md)
 - [PartitionSpec](docs/PartitionSpec.md)
 - [PrimitiveType](docs/PrimitiveType.md)
 - [Reference](docs/Reference.md)
 - [RemovePropertiesUpdate](docs/RemovePropertiesUpdate.md)
 - [RemovePropertiesUpdateAllOf](docs/RemovePropertiesUpdateAllOf.md)
 - [RemoveSnapshotRefUpdate](docs/RemoveSnapshotRefUpdate.md)
 - [RemoveSnapshotsUpdate](docs/RemoveSnapshotsUpdate.md)
 - [RemoveSnapshotsUpdateAllOf](docs/RemoveSnapshotsUpdateAllOf.md)
 - [RenameTableRequest](docs/RenameTableRequest.md)
 - [ReportMetricsRequest](docs/ReportMetricsRequest.md)
 - [ScanReport](docs/ScanReport.md)
 - [Schema](docs/Schema.md)
 - [SchemaAllOf](docs/SchemaAllOf.md)
 - [SetCurrentSchemaUpdate](docs/SetCurrentSchemaUpdate.md)
 - [SetCurrentSchemaUpdateAllOf](docs/SetCurrentSchemaUpdateAllOf.md)
 - [SetDefaultSortOrderUpdate](docs/SetDefaultSortOrderUpdate.md)
 - [SetDefaultSortOrderUpdateAllOf](docs/SetDefaultSortOrderUpdateAllOf.md)
 - [SetDefaultSpecUpdate](docs/SetDefaultSpecUpdate.md)
 - [SetDefaultSpecUpdateAllOf](docs/SetDefaultSpecUpdateAllOf.md)
 - [SetExpression](docs/SetExpression.md)
 - [SetLocationUpdate](docs/SetLocationUpdate.md)
 - [SetLocationUpdateAllOf](docs/SetLocationUpdateAllOf.md)
 - [SetPropertiesUpdate](docs/SetPropertiesUpdate.md)
 - [SetPropertiesUpdateAllOf](docs/SetPropertiesUpdateAllOf.md)
 - [SetSnapshotRefUpdate](docs/SetSnapshotRefUpdate.md)
 - [SetSnapshotRefUpdateAllOf](docs/SetSnapshotRefUpdateAllOf.md)
 - [Snapshot](docs/Snapshot.md)
 - [SnapshotLogInner](docs/SnapshotLogInner.md)
 - [SnapshotReference](docs/SnapshotReference.md)
 - [SnapshotSummary](docs/SnapshotSummary.md)
 - [SortDirection](docs/SortDirection.md)
 - [SortField](docs/SortField.md)
 - [SortOrder](docs/SortOrder.md)
 - [StructField](docs/StructField.md)
 - [StructType](docs/StructType.md)
 - [TableIdentifier](docs/TableIdentifier.md)
 - [TableMetadata](docs/TableMetadata.md)
 - [TableRequirement](docs/TableRequirement.md)
 - [TableUpdate](docs/TableUpdate.md)
 - [Term](docs/Term.md)
 - [TimerResult](docs/TimerResult.md)
 - [TokenType](docs/TokenType.md)
 - [Transform](docs/Transform.md)
 - [TransformTerm](docs/TransformTerm.md)
 - [Type](docs/Type.md)
 - [UnaryExpression](docs/UnaryExpression.md)
 - [UpdateNamespacePropertiesRequest](docs/UpdateNamespacePropertiesRequest.md)
 - [UpdateProperties200Response](docs/UpdateProperties200Response.md)
 - [UpdateTable200Response](docs/UpdateTable200Response.md)
 - [UpgradeFormatVersionUpdate](docs/UpgradeFormatVersionUpdate.md)
 - [UpgradeFormatVersionUpdateAllOf](docs/UpgradeFormatVersionUpdateAllOf.md)


## Documentation For Authorization

## BearerAuth
- **Type**: Bearer token authentication

Example
```
```
## OAuth2
- **Type**: OAuth
- **Flow**: application
- **Authorization URL**: 
- **Scopes**: 
 - **catalog**: Allows interacting with the Config and Catalog APIs

Example
```
```

Or via OAuth2 module to automatically refresh tokens and perform user authentication.
```
```

## Author



