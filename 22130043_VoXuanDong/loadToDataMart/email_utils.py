import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import traceback
from datetime import datetime


# ============================================================
# SEND EMAIL ERROR
# ============================================================

def send_error_email(cfg, subject, error_message, stacktrace=None):
    """Gửi email thông báo lỗi cho admin"""
    try:
        mail_cfg = cfg["email"]

        msg = MIMEMultipart()
        msg["From"] = mail_cfg["sender"]
        msg["To"] = mail_cfg["receiver"]
        msg["Subject"] = subject

        # Tạo email body
        body = f"""
DATA WAREHOUSE ERROR REPORT
================================
Timestamp: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
Environment: Production

Process Details:
- Process Code: {cfg.get('job', {}).get('process_code', 'N/A')}
- Source ID: {cfg.get('job', {}).get('source_id', 'N/A')}
- Control Table: {cfg.get('control_table', 'N/A')}

Error Information:
{error_message}
"""

        if stacktrace:
            body += f"\n\nDetailed Stack Trace:\n{'='*50}\n{stacktrace}\n"
        
        body += "\n\nPlease check the system logs and take necessary action.\n\nBest regards,\nDW Monitoring System"

        msg.attach(MIMEText(body, "plain"))

        # Gửi email
        with smtplib.SMTP(mail_cfg["smtp_host"], mail_cfg["smtp_port"]) as server:
            server.starttls()
            server.login(mail_cfg["username"], mail_cfg["password"])
            server.send_message(msg)

        print("Email notification sent successfully to admin")
        return True

    except Exception as e:
        print(f"Failed to send email notification: {e}")
        print(f"Email config: Host={cfg.get('email', {}).get('smtp_host', 'N/A')}")
        return False


def send_success_email(cfg, subject, process_info):
    """Gửi email thông báo thành công cho admin"""
    try:
        mail_cfg = cfg["email"]

        msg = MIMEMultipart()
        msg["From"] = mail_cfg["sender"]
        msg["To"] = mail_cfg["receiver"]
        msg["Subject"] = subject

        # Tạo email body
        body = f"""
DATA WAREHOUSE SUCCESS REPORT
===================================
Timestamp: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
Environment: Production

Process Details:
{process_info}

All processes completed successfully.

Best regards,
DW Monitoring System
"""

        msg.attach(MIMEText(body, "plain"))

        # Gửi email
        with smtplib.SMTP(mail_cfg["smtp_host"], mail_cfg["smtp_port"]) as server:
            server.starttls()
            server.login(mail_cfg["username"], mail_cfg["password"])
            server.send_message(msg)

        print("Success notification sent to admin")
        return True

    except Exception as e:
        print(f"Failed to send success notification: {e}")
        return False


def send_parallel_summary_email(cfg, bds_success, chotot_success, execution_time, errors=None):
    """Gửi email tổng hợp kết quả parallel execution"""
    try:
        mail_cfg = cfg["email"]
        
        # Xác định trạng thái tổng thể
        overall_status = "SUCCESS" if (bds_success and chotot_success) else "FAILED"
        subject = f"[{overall_status}] DW Parallel Aggregate Summary - {datetime.now().strftime('%Y-%m-%d')}"

        msg = MIMEMultipart()
        msg["From"] = mail_cfg["sender"]
        msg["To"] = mail_cfg["receiver"]
        msg["Subject"] = subject

        # Tạo email body
        body = f"""
DATA WAREHOUSE PARALLEL EXECUTION SUMMARY
==========================================
Timestamp: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
Execution Time: {execution_time:.2f} seconds
Overall Status: {overall_status}

Process Results:
- BDS Aggregate: {'SUCCESS' if bds_success else 'FAILED'}
- ChoTot Aggregate: {'SUCCESS' if chotot_success else 'FAILED'}
"""

        if errors:
            body += "\nError Details:\n"
            if errors.get('bds_error'):
                body += f"- BDS Error: {errors['bds_error']}\n"
            if errors.get('chotot_error'):
                body += f"- ChoTot Error: {errors['chotot_error']}\n"
        
        body += "\n\nBest regards,\nDW Monitoring System"

        msg.attach(MIMEText(body, "plain"))

        # Gửi email
        with smtplib.SMTP(mail_cfg["smtp_host"], mail_cfg["smtp_port"]) as server:
            server.starttls()
            server.login(mail_cfg["username"], mail_cfg["password"])
            server.send_message(msg)

        print(f"Parallel execution summary sent to admin ({overall_status})")
        return True

    except Exception as e:
        print(f"Failed to send parallel summary email: {e}")
        return False
