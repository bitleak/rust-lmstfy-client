use std::cmp::PartialEq;
use std::error;
use std::fmt;

/// Create an `enum` to classify api error.
#[derive(Debug, Clone, PartialEq)]
pub enum ErrType {
    RequestErr,
    ResponseErr,
    Other,
}

impl fmt::Display for ErrType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let printable = match *self {
            ErrType::RequestErr => "req",
            ErrType::ResponseErr => "resp",
            ErrType::Other => "",
        };

        write!(f, "{}", printable)
    }
}

#[derive(Debug, Clone)]
pub struct APIError {
    pub err_type: ErrType,
    pub reason: String,
    pub job_id: String,
    pub request_id: String,
}

impl error::Error for APIError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        None
    }
}

impl fmt::Display for APIError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "t:{}; m:{}; j:{}; r:{}",
            self.err_type, self.reason, self.job_id, self.request_id
        )
    }
}
