#[derive(Debug)]
pub enum GridFSError {
    FileNotFound(),
}

#[derive(Debug)]
pub enum GridFSExtError {
    MongoError(mongodb::error::Error),
    GridFSError(GridFSError),
    IOError(std::io::Error),
}

impl From<GridFSError> for GridFSExtError {
    fn from(err: GridFSError) -> Self {
        GridFSExtError::GridFSError(err)
    }
}

impl From<mongodb_gridfs::GridFSError> for GridFSExtError {
    fn from(err: mongodb_gridfs::GridFSError) -> Self {
        match err {
            mongodb_gridfs::GridFSError::MongoError(err) => GridFSExtError::MongoError(err),
            mongodb_gridfs::GridFSError::FileNotFound() => GridFSError::FileNotFound().into(),
        }
    }
}

impl From<mongodb::error::Error> for GridFSExtError {
    fn from(err: mongodb::error::Error) -> Self {
        GridFSExtError::MongoError(err)
    }
}

impl From<std::io::Error> for GridFSExtError {
    fn from(err: std::io::Error) -> Self {
        GridFSExtError::IOError(err)
    }
}

pub type Result<T> = std::result::Result<T, GridFSExtError>;
