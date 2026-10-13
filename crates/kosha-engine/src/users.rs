//! Users: access keys whose permissions come from policies. They live in
//! the replicated metadata, so every node can authenticate them.

use redb::{ReadableDatabase, ReadableTable};

use crate::engine::Engine;
use crate::error::{Error, Result};
use crate::meta::{Command, MetaStore, Rejection, Step, Tx, USERS, dec, enc, reject};
use crate::types::User;

impl Tx<'_> {
    pub(crate) fn put_user(&mut self, user: User, create: bool) -> Step<()> {
        let mut t = self.txn.open_table(USERS)?;
        let existing = t
            .get(user.access_key.as_bytes())?
            .map(|v| dec::<User>(v.value()))
            .transpose()?;
        match (&existing, create) {
            (Some(_), true) => {
                return reject(Rejection::UserAlreadyExists(format!(
                    "access key {} already exists",
                    user.access_key
                )));
            }
            (None, false) => return reject(Rejection::NoSuchUser),
            _ => {}
        }
        if existing.as_ref().is_none_or(|e| e.name != user.name) {
            for item in t.iter()? {
                let (_, v) = item?;
                let other: User = dec(v.value())?;
                if other.name == user.name && other.access_key != user.access_key {
                    return reject(Rejection::UserAlreadyExists(format!(
                        "a user named {} already exists",
                        user.name
                    )));
                }
            }
        }
        t.insert(user.access_key.as_bytes(), enc(&user)?.as_slice())?;
        Ok(())
    }

    pub(crate) fn delete_user(&mut self, access_key: &str) -> Step<()> {
        let mut t = self.txn.open_table(USERS)?;
        if t.remove(access_key.as_bytes())?.is_none() {
            return reject(Rejection::NoSuchUser);
        }
        Ok(())
    }
}

impl MetaStore {
    pub fn user(&self, access_key: &str) -> Result<Option<User>> {
        let r = self.db.begin_read()?;
        let t = r.open_table(USERS)?;
        t.get(access_key.as_bytes())?
            .map(|v| dec(v.value()))
            .transpose()
    }

    pub fn list_users(&self) -> Result<Vec<User>> {
        let r = self.db.begin_read()?;
        let t = r.open_table(USERS)?;
        let mut out = Vec::new();
        for item in t.iter()? {
            out.push(dec(item?.1.value())?);
        }
        Ok(out)
    }
}

impl Engine {
    /// Create a user. Fails if the access key or the name is taken.
    pub async fn create_user(&self, user: User) -> Result<()> {
        validate_user(&user)?;
        self.propose_cmd(Command::PutUser { user, create: true })
            .await
            .map(drop)
    }

    /// Replace an existing user (policy, enabled flag, secret, name).
    pub async fn update_user(&self, user: User) -> Result<()> {
        validate_user(&user)?;
        self.propose_cmd(Command::PutUser {
            user,
            create: false,
        })
        .await
        .map(drop)
    }

    pub async fn delete_user(&self, access_key: &str) -> Result<()> {
        self.propose_cmd(Command::DeleteUser {
            access_key: access_key.into(),
        })
        .await
        .map(drop)
    }

    /// A user from local state, without a read barrier. Used to
    /// authenticate every request, so it must be cheap.
    pub fn user_local(&self, access_key: &str) -> Result<Option<User>> {
        self.meta().user(access_key)
    }

    pub async fn list_users(&self) -> Result<Vec<User>> {
        self.query_meta(|m| m.list_users()).await
    }

    pub async fn user(&self, access_key: &str) -> Result<User> {
        let k = access_key.to_string();
        self.query_meta(move |m| m.user(&k))
            .await?
            .ok_or(Error::NoSuchUser)
    }
}

fn validate_user(u: &User) -> Result<()> {
    let ok = |s: &str, min: usize, max: usize| {
        (min..=max).contains(&s.len())
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"+=,.@_-/".contains(&b))
    };
    if !ok(&u.access_key, 3, 128) {
        return Err(Error::InvalidRequest(
            "access key must be 3-128 characters of A-Z a-z 0-9 +=,.@_-/".into(),
        ));
    }
    if u.secret_key.len() < 8 || u.secret_key.len() > 128 {
        return Err(Error::InvalidRequest(
            "secret key must be 8-128 characters".into(),
        ));
    }
    if !ok(&u.name, 1, 64) || u.name.contains('/') {
        return Err(Error::InvalidRequest(
            "user name must be 1-64 characters of A-Z a-z 0-9 +=,.@_-".into(),
        ));
    }
    Ok(())
}
