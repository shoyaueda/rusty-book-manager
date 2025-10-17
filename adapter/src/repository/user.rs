use crate::database::{model::user::UserRow, ConnectionPool};
use async_trait::async_trait;
use derive_new::new;
use kernel::model::id::UserId;
use kernel::model::role::Role;
use kernel::model::user::{
    event::{CreateUser, DeleteUser, UpdateUserPassword, UpdateUserRole},
    User,
};
use kernel::repository::user::UserRepository;
use shared::error::{AppError, AppResult};

#[derive(new)]
pub struct UserRepositoryImpl {
    db: ConnectionPool,
}

#[async_trait]
impl UserRepository for UserRepositoryImpl {
    async fn find_current_user(&self, current_user_id: UserId) -> AppResult<Option<User>> {
        let row = sqlx::query_as!(
            UserRow,
            r#"
                SELECT
                u.user_id,
                u.name,
                u.email,
                r.name as role_name,
                u.created_at,
                u.updated_at
                FROM users AS u
                INNER JOIN roles AS r USING(role_id)
                WHERE u.user_id = ? /* ★修正: $1 を ? に置換 */
            "#,
            current_user_id as _
        )
        .fetch_optional(self.db.inner_ref())
        .await
        .map_err(AppError::SpecificOperationError)?;
        match row {
            Some(r) => Ok(Some(User::try_from(r)?)),
            None => Ok(None),
        }
    }

    async fn find_all(&self) -> AppResult<Vec<User>> {
        let users = sqlx::query_as!(
            UserRow,
            r#"
                SELECT
                    u.user_id,
                    u.name,
                    u.email,
                    r.name as role_name,
                    u.created_at,
                    u.updated_at
                FROM users AS u
                INNER JOIN roles AS r USING(role_id)
                ORDER BY u.created_at DESC;
            "#
        )
        .fetch_all(self.db.inner_ref())
        .await
        .map_err(AppError::SpecificOperationError)?
        .into_iter()
        .filter_map(|row| User::try_from(row).ok())
        .collect();
        Ok(users)
    }

    async fn create(&self, event: CreateUser) -> AppResult<User> {
        let user_id = UserId::new();
        let hashed_password = hash_password(&event.password)?;
        // ユーザーを追加するときは管理者ではなく一般のユーザー権限とする
        let role = Role::User;
        let res = sqlx::query!(
            r#"
                INSERT INTO users(user_id, name, email, password_hash, role_id)
                SELECT ?, ?, ?, ?, role_id FROM roles WHERE name = ?; /* ★修正: $1-$5 を ?, ?, ?, ?, ? に置換 */
            "#,
            user_id as _,
            event.name,
            event.email,
            hashed_password,
            role.as_ref()
        )
        .execute(self.db.inner_ref())
        .await
        .map_err(AppError::SpecificOperationError)?;
        if res.rows_affected() < 1 {
            return Err(AppError::NoRowsAffectedError(
                "No user has been created".into(),
            ));
        }
        Ok(User {
            id: user_id,
            name: event.name,
            email: event.email,
            role,
        })
    }

    async fn update_password(&self, event: UpdateUserPassword) -> AppResult<()> {
        let mut tx = self.db.begin().await?;
        let original_password_hash = sqlx::query!(
            r#"
                SELECT password_hash FROM users WHERE user_id = ?; /* ★修正: $1 を ? に置換 */
            "#,
            event.user_id as _
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(AppError::SpecificOperationError)?
        .password_hash;
        // 現在のパスワードが正しいかを検証する
        verify_password(&event.current_password, &original_password_hash)?;
        // 新しいパスワードのハッシュに置き換える
        let new_password_hash = hash_password(&event.new_password)?;
        sqlx::query!(
            r#"
                UPDATE users SET password_hash = ? WHERE user_id = ?; /* ★修正: $2, $1 を ?, ? に置換 */
            "#,
            event.user_id as _,
            new_password_hash,
        )
        .execute(&mut *tx)
        .await
        .map_err(AppError::SpecificOperationError)?;
        tx.commit().await.map_err(AppError::TransactionError)?;
        Ok(())
    }

    async fn update_role(&self, event: UpdateUserRole) -> AppResult<()> {
        let res = sqlx::query!(
            r#"
                UPDATE users
                SET role_id = (
                   SELECT role_id FROM roles WHERE name = ? /* ★修正: $2 を ? に置換 */
                )
                WHERE user_id = ? /* ★修正: $1 を ? に置換 */
            "#,
            event.user_id as _,
            event.role.as_ref()
        )
        .execute(self.db.inner_ref())
        .await
        .map_err(AppError::SpecificOperationError)?;
        if res.rows_affected() < 1 {
            return Err(AppError::EntityNotFound("Specified user not found".into()));
        }
        Ok(())
    }

    async fn delete(&self, event: DeleteUser) -> AppResult<()> {
        let res = sqlx::query!(
            r#"
                DELETE FROM users
                WHERE user_id = ? /* ★修正: $1 を ? に置換 */
            "#,
            event.user_id as _
        )
        .execute(self.db.inner_ref())
        .await
        .map_err(AppError::SpecificOperationError)?;
        if res.rows_affected() < 1 {
            return Err(AppError::EntityNotFound("Specified user not found".into()));
        }
        Ok(())
    }
}

fn hash_password(password: &str) -> AppResult<String> {
    bcrypt::hash(password, bcrypt::DEFAULT_COST).map_err(AppError::from)
}

fn verify_password(password: &str, hash: &str) -> AppResult<()> {
    let valid = bcrypt::verify(password, hash)?;
    if !valid {
        return Err(AppError::UnauthenticatedError);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::UserRepositoryImpl;
    use crate::database::ConnectionPool;
    use kernel::{
        model::{
            id::UserId,
            role::Role,
            user::{
                event::{CreateUser, DeleteUser, UpdateUserPassword, UpdateUserRole},
                User,
            },
        },
        repository::user::UserRepository,
    };
    use std::str::FromStr;

    // ★修正: sqlx::PgPool を sqlx::MySqlPool に置換 ★
    #[sqlx::test(fixtures("common"))]
    async fn test_find_current_user(pool: sqlx::MySqlPool) -> anyhow::Result<()> {
        let repo = UserRepositoryImpl::new(ConnectionPool::new(pool.clone()));
        let current_user_id = UserId::from_str("5b4c96ac-316a-4bee-8e69-cac5eb84ff4c")?;
        let me = repo.find_current_user(current_user_id).await?;
        assert!(me.is_some());
        assert_eq!(
            me,
            Some(User {
                id: UserId::from_str("5b4c96ac-316a-4bee-8e69-cac5eb84ff4c")?,
                email: "eleazar.fig@example.com".into(),
                name: "Eleazar Fig".into(),
                role: Role::Admin,
            })
        );

        Ok(())
    }

    // ★修正: sqlx::PgPool を sqlx::MySqlPool に置換 ★
    #[sqlx::test(fixtures("common"))]
    async fn test_users(pool: sqlx::MySqlPool) -> anyhow::Result<()> {
        let repo = UserRepositoryImpl::new(ConnectionPool::new(pool.clone()));

        // create
        let event = CreateUser {
            name: "Test".into(),
            email: "test@example.com".into(),
            password: "dummy".into(),
        };
        let user = repo.create(event).await?;

        {
            // update_password
            let event = UpdateUserPassword {
                user_id: user.id,
                current_password: "dummy".into(),
                new_password: "new_password".into(),
            };
            repo.update_password(event).await?;

            // update_role
            let event = UpdateUserRole {
                user_id: user.id,
                role: Role::Admin,
            };
            repo.update_role(event).await?;
        }

        // find
        let user_found = repo.find_current_user(user.id).await?;
        assert_eq!(user_found.unwrap().id, user.id);

        let users = repo.find_all().await?;
        assert!(!users.is_empty());

        {
            // delete
            let event = DeleteUser { user_id: user.id };
            repo.delete(event).await?;
        }

        let user_deleted = repo.find_current_user(user.id).await?;
        assert!(user_deleted.is_none());

        Ok(())
    }
}
