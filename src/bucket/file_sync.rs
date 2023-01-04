use std::path::Path;

use async_trait::async_trait;
use futures::StreamExt;
use mongodb::bson::doc;
use mongodb::bson::oid::ObjectId;
use mongodb_gridfs::GridFSBucket;
use tokio::io::AsyncWriteExt;

use crate::bucket::common::GridFSBucketExt;
use crate::error::Result;

/// Extend file operation-related methods to GridFSBucket.
#[async_trait]
pub trait FileSync {
    /// Download file with `filename` from the cloud to `local_path`.
    async fn download_to(
        &self,
        filename: &str,
        local_path: impl AsRef<Path> + Send + Sync,
    ) -> Result<ObjectId>;

    /// Upload file at `local_path` to the cloud with `filename`.
    async fn upload_from(
        &mut self,
        filename: &str,
        local_path: impl AsRef<Path> + Send,
    ) -> Result<ObjectId>;
}

#[async_trait]
impl FileSync for GridFSBucket {
    async fn download_to(
        &self,
        filename: &str,
        local_path: impl AsRef<Path> + Send + Sync,
    ) -> Result<ObjectId> {
        let oid = self.id(filename).await?;
        let mut file = tokio::fs::File::create(local_path).await?;
        let mut cursor = self.open_download_stream(oid).await?;
        while let Some(buffer) = cursor.next().await {
            file.write_all(&buffer).await?;
        }
        Ok(oid)
    }

    async fn upload_from(
        &mut self,
        filename: &str,
        local_path: impl AsRef<Path> + Send,
    ) -> Result<ObjectId> {
        let file = tokio::fs::File::open(local_path).await?.into_std().await;
        let async_file = futures::io::AllowStdIo::new(file);
        let oid = self.upload_from_stream(filename, async_file, None).await?;
        Ok(oid)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use chain_ext::mongodb_gridfs::DatabaseExt;
    use fake::faker::filesystem::en::FileName;
    use fake::Fake;
    use futures::stream::StreamExt;
    use mongodb::Client;
    use tempfile::NamedTempFile;
    use test_utilities::docker::Builder as ContainerBuilder;
    use test_utilities::fs;
    use test_utilities::gridfs;
    use tokio;

    use super::*;

    #[tokio::test]
    async fn test_upload() {
        let mongo_handle = ContainerBuilder::new("mongo")
            .bind_port_as_default(Some("0"), "27017")
            .build_disposable()
            .await;

        let mongo_url = mongo_handle.url();

        let faker = fs::TempFileFaker::new()
            .kind(fs::TempFileKind::Text)
            .include_content(true);
        let temp_file = faker.fake::<fs::TempFile>();
        let link: String = FileName().fake();

        let mut bucket = Client::with_uri_str(mongo_url)
            .await
            .unwrap()
            .database("testdb")
            .clone()
            .bucket(None);
        let oid = bucket.upload_from(&link, temp_file.path).await.unwrap();
        let mut content = Vec::<u8>::new();
        let mut cursor = bucket.open_download_stream(oid).await.unwrap();
        while let Some(buffer) = cursor.next().await {
            content.extend(buffer);
        }
        assert_eq!(content, temp_file.content.unwrap());
    }

    #[tokio::test]
    async fn test_download() {
        let mongo_handle = ContainerBuilder::new("mongo")
            .bind_port_as_default(Some("0"), "27017")
            .build_disposable()
            .await;
        let mongo_url = mongo_handle.url();
        let bucket = Client::with_uri_str(mongo_url)
            .await
            .unwrap()
            .database("testdb")
            .clone()
            .bucket(None);

        let link: String = FileName().fake();
        let faker = gridfs::TempFileFaker::with_bucket(bucket.clone())
            .kind(fs::TempFileKind::Text)
            .len(50..100)
            .include_content(true)
            .name(link.clone());
        let temp_file = faker.fake::<gridfs::TempFile>();

        assert_eq!(temp_file.filename.unwrap(), link);

        let local_download_path = NamedTempFile::new().unwrap().into_temp_path();
        let ret_oid = bucket
            .download_to(&link, &local_download_path)
            .await
            .unwrap();
        assert_eq!(temp_file.id, ret_oid);

        let download_doc = tokio::fs::read_to_string(local_download_path)
            .await
            .unwrap();
        assert_eq!(temp_file.content.unwrap(), download_doc.into_bytes());
    }

    #[tokio::test]
    async fn test_download_big_file() {
        let mongo_handle = ContainerBuilder::new("mongo")
            .bind_port_as_default(Some("0"), "27017")
            .build_disposable()
            .await;
        let mongo_url = mongo_handle.url();
        let bucket = Client::with_uri_str(mongo_url)
            .await
            .unwrap()
            .database("testdb")
            .clone()
            .bucket(None);

        let link: String = FileName().fake();
        let faker = gridfs::TempFileFaker::with_bucket(bucket.clone())
            .len((399 * 1024)..(400 * 1024))
            .kind(fs::TempFileKind::Text)
            .include_content(true)
            .name(link.clone());
        let temp_file = faker.fake::<gridfs::TempFile>();

        assert_eq!(temp_file.filename.unwrap(), link);

        let local_download_path = NamedTempFile::new().unwrap().into_temp_path();
        let ret_oid = bucket
            .download_to(&link, &local_download_path)
            .await
            .unwrap();
        assert_eq!(temp_file.id, ret_oid);

        let download_doc = tokio::fs::read_to_string(local_download_path)
            .await
            .unwrap();

        let expect_content = temp_file.content.unwrap();
        let downloaded_content = download_doc.into_bytes();

        assert_eq!(expect_content.len(), downloaded_content.len());
        assert_eq!(expect_content, downloaded_content);
    }
}
