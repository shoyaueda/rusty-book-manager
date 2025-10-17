use shared::{
    config::DatabaseConfig,
    error::{AppError, AppResult},
};
// ★★★ 修正点 1: PgConnectOptions, PgPool を MySqlConnectOptions, MySqlPool に変更 ★★★
use sqlx::{mysql::MySqlConnectOptions, MySqlPool};

pub mod model;

// ★★★ 修正点 2: make_pg_connect_options を make_mysql_connect_options に変更 ★★★
fn make_mysql_connect_options(cfg: &DatabaseConfig) -> MySqlConnectOptions {
    MySqlConnectOptions::new()
        .host(&cfg.host)
        .port(cfg.port)
        .username(&cfg.username)
        .password(&cfg.password)
        .database(&cfg.database)
}

#[derive(Clone)]
// ★★★ 修正点 3: PgPool を MySqlPool に変更 ★★★
pub struct ConnectionPool(MySqlPool);

impl ConnectionPool {
    // ★★★ 修正点 4: PgPool を MySqlPool に変更 ★★★
    pub fn new(pool: MySqlPool) -> Self {
        Self(pool)
    }

    // ★★★ 修正点 5: PgPool を MySqlPool に変更 ★★★
    pub fn inner_ref(&self) -> &MySqlPool {
        &self.0
    }

    // ★★★ 修正点 6: sqlx::Postgres を sqlx::MySql に変更 ★★★
    pub async fn begin(&self) -> AppResult<sqlx::Transaction<'_, sqlx::MySql>> {
        self.0.begin().await.map_err(AppError::TransactionError)
    }
}

// ★★★ 修正点 7: PgPool::connect_lazy_with と make_mysql_connect_options に変更 ★★★
pub fn connect_database_with(cfg: &DatabaseConfig) -> ConnectionPool {
    ConnectionPool(MySqlPool::connect_lazy_with(make_mysql_connect_options(cfg)))
}
