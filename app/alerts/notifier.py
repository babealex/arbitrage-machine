from __future__ import annotations

import smtplib
from email.message import EmailMessage

import requests

from app.config import Settings


class Notifier:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def send(self, subject: str, body: str) -> None:
        if self.settings.alert_webhook_url:
            requests.post(self.settings.alert_webhook_url, json={"text": f"{subject}\n{body}"}, timeout=10)
        if self.settings.smtp_host and self.settings.alert_email_from and self.settings.alert_email_to:
            message = EmailMessage()
            message["From"] = self.settings.alert_email_from
            message["To"] = self.settings.alert_email_to
            message["Subject"] = subject
            message.set_content(body)
            with smtplib.SMTP(self.settings.smtp_host, self.settings.smtp_port, timeout=10) as smtp:
                smtp.starttls()
                if self.settings.smtp_username:
                    smtp.login(self.settings.smtp_username, self.settings.smtp_password)
                smtp.send_message(message)
