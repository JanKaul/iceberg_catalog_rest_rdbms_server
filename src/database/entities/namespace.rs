//! SeaORM Entity. Generated by sea-orm-codegen 0.10.0

use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq)]
#[sea_orm(table_name = "namespace")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i32,
    pub name: String,
    pub catalog_id: i32,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::catalog::Entity",
        from = "Column::CatalogId",
        to = "super::catalog::Column::Id",
        on_update = "NoAction",
        on_delete = "NoAction"
    )]
    Catalog,
    #[sea_orm(has_many = "super::iceberg_table::Entity")]
    IcebergTable,
}

impl Related<super::catalog::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Catalog.def()
    }
}

impl Related<super::iceberg_table::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::IcebergTable.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
