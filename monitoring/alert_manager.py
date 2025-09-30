# monitoring/alert_manager.py
import logging
import smtplib
from email.mime.text import MIMEText

ALERT_EMAIL = "alerts@company.com"
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_USER = "your_email@gmail.com"
SMTP_PASS = "your_password"

def send_alert(message="Pipeline alert triggered!"):
    """Sends alert email (can be extended to Slack/Teams)."""
    try:
        msg = MIMEText(message)
        msg["Subject"] = "🚨 Data Pipeline Alert"
        msg["From"] = SMTP_USER
        msg["To"] = ALERT_EMAIL

        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASS)
            server.sendmail(SMTP_USER, ALERT_EMAIL, msg.as_string())

        logging.info(f"[Alert Manager] Alert sent: {message}")
    except Exception as e:
        logging.error(f"[Alert Manager] Failed to send alert: {str(e)}")
        raise

