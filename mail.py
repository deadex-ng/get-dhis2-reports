from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib

def send_report_summary_email(success_reports, failed_reports):
    msg = MIMEMultipart()
    msg['Subject'] = 'Report Pull Summary'
    msg['From'] = 'thisisfumba@gmail.com'
    msg['To'] = 'fubanda@pih.org'

    body = "✅ **Successful Reports:**\n" + "\n".join(success_reports)
    if failed_reports:
        body += "\n\n❌ **Failed Reports:**\n" + "\n".join(failed_reports)
    else:
        body += "\n\nAll reports pulled successfully!"

    msg.attach(MIMEText(body, 'plain'))

    # Send email (using Gmail here)
    with smtplib.SMTP('smtp.gmail.com', 587) as server:
        server.starttls()
        server.login('thisisfumba@gmail.com', 'dlxq dapd ghmf okrz')
        server.send_message(msg)

if __name__ == "__main__":
    send_report_summary_email(['hey'],['fail'])