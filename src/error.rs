use std::error::Error;
use std::fmt::{Display, Formatter};

use mongodb::bson::oid::ObjectId;

#[derive(Debug)]
pub enum GridFSError {
    FileNotFound {
        filename: Option<String>,
        id: Option<ObjectId>,
    },
}

impl Display for GridFSError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FileNotFound {
                filename: Some(filename),
                ..
            } => write!(f, "FileNotFound(filename={})", filename),
            Self::FileNotFound { id: Some(id), .. } => write!(f, "FileNotFound(id={})", id),
            Self::FileNotFound { .. } => write!(f, "FileNotFound(None)"),
        }
    }
}

impl Error for GridFSError {}

#[derive(Debug)]
pub enum GridFSExtError {
    MongoError(mongodb::error::Error),
    GridFSError(GridFSError),
    IOError(std::io::Error),
}

impl Display for GridFSExtError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MongoError(err) => write!(f, "MongoError({})", err),
            Self::GridFSError(err) => write!(f, "GridFSError({})", err),
            Self::IOError(err) => write!(f, "IOError({})", err),
        }
    }
}

impl Error for GridFSExtError {}

impl From<GridFSError> for GridFSExtError {
    fn from(err: GridFSError) -> Self {
        GridFSExtError::GridFSError(err)
    }
}

impl From<mongodb_gridfs::GridFSError> for GridFSExtError {
    fn from(err: mongodb_gridfs::GridFSError) -> Self {
        match err {
            mongodb_gridfs::GridFSError::MongoError(err) => GridFSExtError::MongoError(err),
            mongodb_gridfs::GridFSError::FileNotFound() => GridFSError::FileNotFound {
                filename: None,
                id: None,
            }
            .into(),
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
