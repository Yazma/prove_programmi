import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import os
import configparser
COMMASPACE = ', '


def send_mail(text_for_mail):
    path_file_cfg = os.path.abspath(os.path.join(__file__, "..", "config", "config_WB.ini"))
    cfg = configparser.RawConfigParser()
    cfg.read(path_file_cfg)
    sender = cfg['MAIL']['sender']
    sender_psw = cfg['MAIL']['psw']
    receiver = cfg['MAIL']['receiver']
    receiver = receiver.split(',')
    # receiver = ['adriano.caruso87@gmail.com', 'adriano.caruso@eng.it']
    subject = cfg['MAIL']['subject']
    if text_for_mail:
        body_txt = text_for_mail
    else:
        body_txt = cfg['MAIL']['body']
        body_txt = body_txt + ' https://github.com/pcm-dpc/COVID-19/raw/master/dati-regioni/dpc-covid19-ita-regioni.csv'
    message = MIMEMultipart('alternative')
    message['From'] = sender
    message['To'] = COMMASPACE.join(receiver)
    message['Subject'] = subject
    email_body = MIMEText(body_txt, 'plain')
    message.attach(email_body)
    try:
        session = smtplib.SMTP('smtp-mail.outlook.com')
        session.starttls()
        session.login(sender, sender_psw)  # login with mail_id and password
        body = message.as_string()
        session.sendmail(sender, receiver, body)
        session.close()
        print('Success sending from', sender, 'to', receiver)
        print("Email content: ", body_txt)
    except Exception as e:
        print('Error during connection to Outlook account')
        print(e)


if __name__ == '__main__':
    text_for_mail = str()
    send_mail(text_for_mail)
