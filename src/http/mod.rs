pub mod axum;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum HttpMethod {
    Get,
    Post,
    Put,
    Patch,
    Delete,
}

impl HttpMethod {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Post => "POST",
            Self::Put => "PUT",
            Self::Patch => "PATCH",
            Self::Delete => "DELETE",
        }
    }
}

impl Into<&'static str> for HttpMethod {
    fn into(self) -> &'static str {
        self.as_str()
    }
}

impl Into<HttpMethod> for &'static str {
    fn into(self) -> HttpMethod {
        match self {
            "GET" => HttpMethod::Get,
            "POST" => HttpMethod::Post,
            "PUT" => HttpMethod::Put,
            "DELETE" => HttpMethod::Delete,
            "PATCH" => HttpMethod::Patch,
            _ => HttpMethod::Get,
        }
    }
}

impl std::fmt::Display for HttpMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct HttpEndpoint {
    pub method: HttpMethod,
    pub path: String,
}

impl HttpEndpoint {
    pub fn new(method: HttpMethod, path: impl Into<String>) -> Self {
        Self {
            method,
            path: path.into(),
        }
    }

    pub fn get(path: impl Into<String>) -> Self {
        Self::new(HttpMethod::Get, path)
    }

    pub fn post(path: impl Into<String>) -> Self {
        Self::new(HttpMethod::Post, path)
    }

    pub fn put(path: impl Into<String>) -> Self {
        Self::new(HttpMethod::Put, path)
    }

    pub fn patch(path: impl Into<String>) -> Self {
        Self::new(HttpMethod::Patch, path)
    }

    pub fn delete(path: impl Into<String>) -> Self {
        Self::new(HttpMethod::Delete, path)
    }
}

impl From<(HttpMethod, String)> for HttpEndpoint {
    fn from((method, path): (HttpMethod, String)) -> Self {
        Self::new(method, path)
    }
}

impl From<(HttpMethod, &str)> for HttpEndpoint {
    fn from((method, path): (HttpMethod, &str)) -> Self {
        Self::new(method, path)
    }
}

pub type HttpRoute = HttpEndpoint;
