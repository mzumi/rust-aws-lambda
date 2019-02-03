#[macro_use]
extern crate lambda_runtime as lambda;
#[macro_use]
extern crate serde_derive;

extern crate log;
extern crate simple_logger;

use lambda::error::HandlerError;

use std::env;
use std::error;
use std::error::Error;
use std::fmt;

extern crate zip;
use std::io::Write;

use std::path::Path;

use std::str;

extern crate rusoto_core;
extern crate rusoto_s3;

use std::default::Default;

use rusoto_core::{ByteStream, Region};
use rusoto_s3::{GetObjectRequest, PutObjectRequest, S3Client, S3};

use flate2::write::GzEncoder;
use flate2::Compression;
use futures::stream::Stream;

fn main() -> Result<(), Box<dyn Error>> {
    simple_logger::init_with_level(log::Level::Info)?;
    lambda!(my_handler);

    Ok(())
}

fn my_handler(e: S3Event, c: lambda::Context) -> Result<(), HandlerError> {
    let s3 = S3Client::new(Region::ApNortheast1);
    for rec in &e.records {
        let enc = compress(&s3, rec).map_err(|e| c.new_error(e.description()))?;

        let _ = upload_file(&s3, rec, enc);
    }

    Ok(())
}

fn compress(s3: &S3Client, record: &S3Record) -> Result<Vec<u8>, CompressorError> {
    let file_name = &record.s3.object.key;

    let mut request = GetObjectRequest::default();
    request.bucket = record.s3.bucket.name.clone();
    request.key = file_name.clone();

    let object = s3.get_object(request).sync()?;
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());

    if let Some(stream) = object.body.map(|b| b.take(512 * 1024).wait()) {
        for chunck in stream {
            encoder.write_all(chunck?.as_mut_slice()).unwrap();
        }
    }

    encoder.finish().map_err(|e| From::from(e))
}

fn upload_file(s3: &S3Client, record: &S3Record, enc: Vec<u8>) -> Result<(), CompressorError> {
    let to_backet = env::var("TO_BACKET")?;
    let put_req = PutObjectRequest {
        bucket: to_backet,
        key: format!(
            "{}.gz",
            Path::new(&record.s3.object.key)
                .file_stem()
                .unwrap()
                .to_string_lossy()
        ),
        body: Some(ByteStream::from(enc)),
        ..Default::default()
    };

    s3.put_object(put_req).sync()?;

    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct S3Event {
    #[serde(rename = "Records")]
    records: Vec<S3Record>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct S3Record {
    s3: S3Info,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct S3Info {
    object: S3Object,
    bucket: S3Bucket,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct S3Object {
    key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct S3Bucket {
    name: String,
}

#[derive(Debug)]
enum CompressorError {
    IoError(std::io::Error),
    Zip(zip::result::ZipError),
    PutObjectError(rusoto_s3::PutObjectError),
    GetObjectError(rusoto_s3::GetObjectError),
    VarError(std::env::VarError),
}

impl fmt::Display for CompressorError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            CompressorError::Zip(ref e) => write!(f, "Zip error: {}", e),
            CompressorError::IoError(ref e) => write!(f, "IO error: {}", e),
            CompressorError::PutObjectError(ref e) => write!(f, "S3 put error: {}", e),
            CompressorError::GetObjectError(ref e) => write!(f, "S3 get error: {}", e),
            CompressorError::VarError(ref e) => write!(f, "environment variable error: {}", e),
        }
    }
}

impl error::Error for CompressorError {
    fn description(&self) -> &str {
        match *self {
            CompressorError::Zip(ref e) => e.description(),
            CompressorError::IoError(ref e) => e.description(),
            CompressorError::PutObjectError(ref e) => e.description(),
            CompressorError::GetObjectError(ref e) => e.description(),
            CompressorError::VarError(ref e) => e.description(),
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            CompressorError::Zip(ref e) => Some(e),
            CompressorError::IoError(ref e) => Some(e),
            CompressorError::PutObjectError(ref e) => Some(e),
            CompressorError::GetObjectError(ref e) => Some(e),
            CompressorError::VarError(ref e) => Some(e),
        }
    }
}

impl From<zip::result::ZipError> for CompressorError {
    fn from(e: zip::result::ZipError) -> CompressorError {
        CompressorError::Zip(e)
    }
}

impl From<std::io::Error> for CompressorError {
    fn from(e: std::io::Error) -> CompressorError {
        CompressorError::IoError(e)
    }
}

impl From<rusoto_s3::PutObjectError> for CompressorError {
    fn from(e: rusoto_s3::PutObjectError) -> CompressorError {
        CompressorError::PutObjectError(e)
    }
}

impl From<rusoto_s3::GetObjectError> for CompressorError {
    fn from(e: rusoto_s3::GetObjectError) -> CompressorError {
        CompressorError::GetObjectError(e)
    }
}

impl From<std::env::VarError> for CompressorError {
    fn from(e: std::env::VarError) -> CompressorError {
        CompressorError::VarError(e)
    }
}
