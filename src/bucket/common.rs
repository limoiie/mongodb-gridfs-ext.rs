use async_trait::async_trait;
use futures::StreamExt;
use mongodb::bson::doc;
use mongodb::bson::oid::ObjectId;
use mongodb_gridfs::options::{GridFSFindOptions, GridFSUploadOptions};
use mongodb_gridfs::{GridFSBucket, GridFSError};

use crate::error::Result;

/// Extend common helper methods to [GridFSBucket].
#[async_trait]
pub trait GridFSBucketExt {
    /// Get doc id by `filename`.
    async fn id<S>(&self, filename: S) -> Result<ObjectId>
    where
        S: AsRef<str> + Send;

    /// Get doc filename by `id`.
    async fn filename(&self, id: ObjectId) -> Result<String>;

    /// Read cloud file by `id` as [alloc::String].
    async fn read_string<S>(&self, filename: S) -> Result<String>
    where
        S: AsRef<str> + Send;

    /// Read cloud file by `id` as [alloc::Vec<u8>].
    async fn read_bytes<S>(&self, filename: S) -> Result<Vec<u8>>
    where
        S: AsRef<str> + Send;

    /// Write [&str] into cloud file by `id`.
    async fn write_string<S>(&mut self, filename: S, content: &str) -> Result<()>
    where
        S: AsRef<str> + Send + Sync;

    /// Write [&\[u8\]] into cloud file by `id`.
    async fn write_bytes<S: AsRef<str> + Send + Sync>(
        &mut self,
        filename: S,
        content: &[u8],
    ) -> Result<()>;

    /// Return true if there is a file on the cloud with `filename`.
    async fn exists<S: AsRef<str> + Send>(&self, filename: S) -> Result<bool>;
}

#[async_trait]
impl GridFSBucketExt for GridFSBucket {
    async fn id<S>(&self, filename: S) -> Result<ObjectId>
    where
        S: AsRef<str> + Send,
    {
        let opt = GridFSFindOptions::default();
        self.find(doc! {"filename": filename.as_ref()}, opt)
            .await?
            .next()
            .await
            .ok_or(GridFSError::FileNotFound())?
            .map(|doc| doc.get_object_id("_id").unwrap())
            .map_err(Into::into)
    }

    async fn filename(&self, id: ObjectId) -> Result<String> {
        let opt = GridFSFindOptions::default();
        self.find(doc! {"_id": id}, opt)
            .await?
            .next()
            .await
            .ok_or(GridFSError::FileNotFound())?
            .map(|doc| doc.get_str("filename").unwrap().to_owned())
            .map_err(Into::into)
    }

    async fn read_string<S>(&self, filename: S) -> Result<String>
    where
        S: AsRef<str> + Send,
    {
        self.read_bytes(filename)
            .await
            .and_then(|bytes| std::io::read_to_string(bytes.as_slice()).map_err(|err| err.into()))
    }

    async fn read_bytes<S>(&self, filename: S) -> Result<Vec<u8>>
    where
        S: AsRef<str> + Send,
    {
        let id = self.id(filename).await?;
        let mut cursor = self.open_download_stream(id).await?;
        let buffer = cursor.next().await.unwrap_or_default();
        Ok(buffer)
    }

    async fn write_string<S>(&mut self, filename: S, content: &str) -> Result<()>
    where
        S: AsRef<str> + Send + Sync,
    {
        self.write_bytes(filename, content.as_bytes()).await
    }

    async fn write_bytes<S>(&mut self, filename: S, content: &[u8]) -> Result<()>
    where
        S: AsRef<str> + Send + Sync,
    {
        let opt = GridFSUploadOptions::default();
        self.upload_from_stream(filename.as_ref(), content, Some(opt))
            .await?;
        Ok(())
    }

    async fn exists<S>(&self, filename: S) -> Result<bool>
    where
        S: AsRef<str> + Send,
    {
        let opt = GridFSFindOptions::default();
        let mut cursor = self.find(doc! {"filename": filename.as_ref()}, opt).await?;
        Ok(cursor.next().await.is_some())
    }
}

#[cfg(test)]
mod tests {
    use fake::Fake;
    use mongodb::Client;
    use test_utilities::docker;
    use test_utilities::gridfs::{TempFile, TempFileFaker};

    use crate::error::GridFSError::FileNotFound;
    use crate::error::GridFSExtError::GridFSError;

    use super::*;

    #[tokio::test]
    async fn test_id() {
        let filename = "some-filename.txt";

        let handle = docker::Builder::new("mongo")
            .port_mapping(0, Some(27017))
            .build_disposable()
            .await;

        let bucket = GridFSBucket::new(
            Client::with_uri_str(handle.url.as_ref().unwrap())
                .await
                .unwrap()
                .database("test_db"),
            None,
        );

        let file = TempFileFaker::with_bucket(bucket.clone())
            .name(filename.into())
            .fake::<TempFile>();

        assert_eq!(file.id, bucket.id(filename).await.unwrap());
        match bucket.id("non-exist-filename.txt").await.unwrap_err() {
            GridFSError(FileNotFound()) => (),
            _ => assert!(false, "Should return error [GridFSError(FileNotFound())]"),
        }
    }

    #[tokio::test]
    async fn test_read_string() {
        let filename = "some-filename.txt";

        let handle = docker::Builder::new("mongo")
            .port_mapping(0, Some(27017))
            .build_disposable()
            .await;

        let bucket = GridFSBucket::new(
            Client::with_uri_str(handle.url.as_ref().unwrap())
                .await
                .unwrap()
                .database("test_db"),
            None,
        );

        let file = TempFileFaker::with_bucket(bucket.clone())
            .name(filename.into())
            .include_content(true)
            .fake::<TempFile>();

        assert_eq!(
            file.content.unwrap().as_slice(),
            bucket.read_string(filename).await.unwrap().as_bytes()
        );
    }

    #[tokio::test]
    async fn test_read_bytes() {
        let filename = "some-filename.txt";

        let handle = docker::Builder::new("mongo")
            .port_mapping(0, Some(27017))
            .build_disposable()
            .await;

        let bucket = GridFSBucket::new(
            Client::with_uri_str(handle.url.as_ref().unwrap())
                .await
                .unwrap()
                .database("test_db"),
            None,
        );

        let file = TempFileFaker::with_bucket(bucket.clone())
            .name(filename.into())
            .include_content(true)
            .fake::<TempFile>();

        assert_eq!(
            file.content.unwrap().as_slice(),
            bucket.read_bytes(filename).await.unwrap()
        );
    }

    #[tokio::test]
    async fn test_exists() {
        let filename = "some-filename.txt";

        let handle = docker::Builder::new("mongo")
            .port_mapping(0, Some(27017))
            .build_disposable()
            .await;

        let bucket = GridFSBucket::new(
            Client::with_uri_str(handle.url.as_ref().unwrap())
                .await
                .unwrap()
                .database("test_db"),
            None,
        );

        let _file = TempFileFaker::with_bucket(bucket.clone())
            .name(filename.into())
            .fake::<TempFile>();

        assert!(bucket.exists(filename).await.unwrap());
        assert!(!bucket.exists("non-exist-filename.txt").await.unwrap());
    }
}
