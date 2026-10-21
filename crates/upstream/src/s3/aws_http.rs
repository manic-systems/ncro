use aws_smithy_runtime_api::{
  client::{
    http::{
      HttpClient,
      HttpConnector,
      HttpConnectorFuture,
      HttpConnectorSettings,
      SharedHttpConnector,
    },
    orchestrator::{HttpRequest, HttpResponse},
    result::ConnectorError,
    runtime_components::RuntimeComponents,
  },
  http::{Headers, StatusCode},
};
use aws_smithy_types::body::SdkBody;
use reqwest::Method;

/// Serves the AWS credential providers (STS, SSO, IMDS, ECS) over reqwest.
#[derive(Debug)]
pub struct ReqwestHttpClient;

impl HttpClient for ReqwestHttpClient {
  fn http_connector(
    &self,
    settings: &HttpConnectorSettings,
    _components: &RuntimeComponents,
  ) -> SharedHttpConnector {
    let mut builder = reqwest::Client::builder();
    if let Some(timeout) = settings.connect_timeout() {
      builder = builder.connect_timeout(timeout);
    }
    if let Some(timeout) = settings.read_timeout() {
      builder = builder.read_timeout(timeout);
    }
    SharedHttpConnector::new(ReqwestConnector(builder.build()))
  }
}

#[derive(Debug)]
struct ReqwestConnector(reqwest::Result<reqwest::Client>);

impl HttpConnector for ReqwestConnector {
  fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
    let method = match Method::from_bytes(request.method().as_bytes()) {
      Ok(method) => method,
      Err(err) => {
        return HttpConnectorFuture::ready(Err(ConnectorError::user(
          Box::new(err),
        )));
      },
    };
    let client = match &self.0 {
      Ok(client) => client,
      Err(err) => {
        return HttpConnectorFuture::ready(Err(ConnectorError::other(
          err.to_string().into(),
          None,
        )));
      },
    };
    let Some(body) = request.body().bytes() else {
      return HttpConnectorFuture::ready(Err(ConnectorError::user(
        "streaming request bodies are not supported".into(),
      )));
    };

    let mut builder = client.request(method, request.uri()).body(body.to_vec());
    for (name, value) in request.headers() {
      builder = builder.header(name, value);
    }

    HttpConnectorFuture::new(async move {
      let resp = builder.send().await.map_err(connector_error)?;
      let status = StatusCode::try_from(resp.status().as_u16())
        .map_err(|err| ConnectorError::other(Box::new(err), None))?;
      let mut headers = Headers::new();
      for (name, value) in resp.headers() {
        if let Ok(value) = value.to_str() {
          headers.append(name.as_str().to_owned(), value.to_owned());
        }
      }
      let body = resp.bytes().await.map_err(connector_error)?;

      let mut out = HttpResponse::new(status, SdkBody::from(body));
      *out.headers_mut() = headers;
      Ok(out)
    })
  }
}

fn connector_error(err: reqwest::Error) -> ConnectorError {
  if err.is_timeout() {
    ConnectorError::timeout(Box::new(err))
  } else if err.is_connect() || err.is_body() {
    ConnectorError::io(Box::new(err))
  } else {
    ConnectorError::other(Box::new(err), None)
  }
}
