use async_trait::async_trait;
use futures::StreamExt;
use mongodb::bson::doc;
use mongodb::bson::oid::ObjectId;
use mongodb_gridfs::options::GridFSFindOptions;
use mongodb_gridfs::{GridFSBucket, GridFSError};

use crate::error::Result;

#[async_trait]
pub trait GridFSBucketExt {
    async fn id(&self, filename: &str) -> Result<ObjectId>;
    async fn read_as_string(&self, id: ObjectId) -> Result<String>;
    async fn read_as_bytes(&self, id: ObjectId) -> Result<Vec<u8>>;
    async fn exists(&self, filename: &str) -> Result<bool>;
}

#[async_trait]
impl GridFSBucketExt for GridFSBucket {
    async fn id(&self, filename: &str) -> Result<ObjectId> {
        let opt = GridFSFindOptions::default();
        self.find(doc! {"filename": filename}, opt)
            .await?
            .next()
            .await
            .ok_or(GridFSError::FileNotFound())?
            .map(|doc| doc.get_object_id("_id").unwrap())
            .map_err(Into::into)
    }

    async fn read_as_string(&self, id: ObjectId) -> Result<String> {
        self.read_as_bytes(id)
            .await
            .and_then(|bytes| std::io::read_to_string(bytes.as_slice()).map_err(|err| err.into()))
    }

    async fn read_as_bytes(&self, id: ObjectId) -> Result<Vec<u8>> {
        let mut cursor = self.open_download_stream(id).await?;
        let buffer = cursor.next().await.ok_or(GridFSError::FileNotFound())?;
        Ok(buffer)
    }

    async fn exists(&self, filename: &str) -> Result<bool> {
        let opt = GridFSFindOptions::default();
        let mut cursor = self.find(doc! {"filename": filename}, opt).await?;
        Ok(cursor.next().await.is_some())
    }
}
