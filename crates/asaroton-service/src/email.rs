//! Sending mail: the verification of email addresses on accounts (`[email]`).

use lettre::{
    AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor,
    message::{Mailbox, header::ContentType},
    transport::smtp::authentication::Credentials,
};

use crate::{
    Error, Result, Services,
    config::{EmailSettings, EmailTransport, SmtpTls},
};

impl Services {
    /// Send a plain-text mail to `to`. Fails with a 500 when the mail could not be handed
    /// over (the reason is logged, not shown).
    pub async fn send_mail(&self, to: &str, subject: &str, body: &str) -> Result<()> {
        let Some(settings) = &self.config.email else {
            return Err(Error::internal("mail is not configured"));
        };
        send(settings, to, subject, body).await.map_err(|e| {
            tracing::warn!(error = %e, "sending mail failed");
            Error::internal("The mail could not be sent.")
        })
    }
}

type SendError = Box<dyn std::error::Error + Send + Sync>;

async fn send(
    settings: &EmailSettings,
    to: &str,
    subject: &str,
    body: &str,
) -> std::result::Result<(), SendError> {
    let from: Mailbox = settings.from.parse()?;
    let to: Mailbox = to.parse()?;
    let message = Message::builder()
        .from(from)
        .to(to)
        .subject(subject)
        .header(ContentType::TEXT_PLAIN)
        .body(body.to_owned())?;
    match &settings.transport {
        EmailTransport::Smtp {
            host,
            port,
            tls,
            username,
            password,
        } => {
            let mut builder = match tls {
                SmtpTls::None => AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous(host),
                SmtpTls::StartTls => AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(host)?,
                SmtpTls::Tls => AsyncSmtpTransport::<Tokio1Executor>::relay(host)?,
            }
            .port(*port);
            if let (Some(u), Some(p)) = (username, password) {
                builder = builder.credentials(Credentials::new(u.clone(), p.clone()));
            }
            builder.build().send(message).await?;
        }
        EmailTransport::Directory(dir) => {
            tokio::fs::create_dir_all(dir).await?;
            let name = format!(
                "{}-{}.eml",
                asaroton_store::now_ts(),
                asaroton_auth::generate_token("m")
            );
            tokio::fs::write(dir.join(name), message.formatted()).await?;
        }
    }
    Ok(())
}
