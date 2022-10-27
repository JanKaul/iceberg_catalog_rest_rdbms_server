use sea_orm::{ConnectionTrait, DatabaseConnection};
use sea_orm::{Database, DbBackend, DbErr, Statement};
use sea_orm_migration::prelude::*;

pub mod entities;
mod migrator;

const DATABASE_URL: &str = "postgres://postgres:postgres@localhost:5432";
const DB_NAME: &str = "iceberg_catalog";

pub(crate) async fn run() -> Result<DatabaseConnection, DbErr> {
    let db = Database::connect(DATABASE_URL).await?;

    let db = match db.get_database_backend() {
        DbBackend::MySql => {
            db.execute(Statement::from_string(
                db.get_database_backend(),
                format!("CREATE DATABASE IF NOT EXISTS `{}`;", DB_NAME),
            ))
            .await?;

            db.execute(Statement::from_string(
                db.get_database_backend(),
                format!("SET GLOBAL TRANSACTION ISOLATION LEVEL SERIALIZABLE"),
            ))
            .await?;

            let url = format!("{}/{}", DATABASE_URL, DB_NAME);
            Database::connect(&url).await?
        }
        DbBackend::Postgres => {
            let _ = db
                .execute(Statement::from_string(
                    db.get_database_backend(),
                    format!("CREATE DATABASE {}", DB_NAME),
                ))
                .await;

            db.execute(Statement::from_string(
                db.get_database_backend(),
                format!(
                    "ALTER DATABASE {} SET DEFAULT_TRANSACTION_ISOLATION TO SERIALIZABLE",
                    DB_NAME
                ),
            ))
            .await?;

            let url = format!("{}/{}", DATABASE_URL, DB_NAME);
            Database::connect(&url).await?
        }
        DbBackend::Sqlite => db,
    };

    migrator::Migrator::refresh(&db).await?;

    Ok(db)
}
