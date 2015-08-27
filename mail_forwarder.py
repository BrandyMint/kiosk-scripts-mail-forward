#!/usr/bin/python
# -*- coding: utf8 -*-

"""
[#101189506] Скрипт форвордящий сообщения с ящиков username@kiiiosk.ru на support_email из базы.
Пример secrets:

mail_forwarder:
  debug: True
  imap:
    server: imap.yandex.ru
    timeout: 10
    debug_level: 4
    port: 993
    login: mail-forward@kiiiosk.ru
    password: secret
  smtp:
    server: localhost
    port: 25
    from: support@kiiiosk.ru

Использовать:
  
  MERCHANTLY_DIR=/home/wwwkiiiosk/kiiiosk.ru/current RAILS_ENV=production ./mail_forwarder.py
"""


import imaplib
import smtplib
import email
import syslog
import yaml
import sys
import signal
import psycopg2
from psycopg2.extras import NamedTupleConnection
from socket import setdefaulttimeout
from os import stat, getenv
from os.path import isfile
from ssl import SSLError
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email.mime.text import MIMEText
from email.parser import Parser
from email.utils import parseaddr

def sigint_handler(signal, frame):
  print('\nYou pressed Ctrl+C!')
  sys.exit(0)

def get_conf(t, var=None):
  env = getenv('RAILS_ENV', default='development')
  if t == 'secrets':
    return conf.get(t).get(env).get('mail_forwarder').get(var)
  elif t == 'database':
    return conf.get(t).get(env).get(var)

def mailer(buf, support_email):
  eo  = email.parser.Parser()
  msg = eo.parsestr(buf)
  sender    = msg.get('Return-Path')#[1:-1]
  recipient = msg.get('To')
  subject = msg.get('Subject')


  bouncetxt = """Здравствуйте!
    Вам было отправлено письмо на адрес %(recipient)s от: %(sender)s %(subject)s.""" % {'recipient': recipient, 'sender': sender, 'subject': (subject and 'с темой: %s' % subject or '' )}
  bouncetxt +="""
  Сообщение прилагается.
  """ 

  bounce = MIMEMultipart()
  bounce["From"]    = recipient
  bounce["To"]      = support_email
  bounce['Return-Path'] = sender
  bounce['Reply-To'] = sender
  bounce["Subject"] = 'Пересылка письма от %s' % sender

  bounce.attach(MIMEText(bouncetxt, _charset='utf-8'))
  bounce.attach(msg)

  server = smtplib.SMTP('localhost')
  server.sendmail(recipient, support_email, bounce.as_string())
  server.quit()
  syslog.syslog('msg from=%s to=%s forwarded'%(sender,recipient))

def main():
  global conf
  conf = {}

  for item in ['database', 'secrets']:
    try:
      with open('%s/config/%s.yml' % (getenv('MERCHANTLY_DIR', default='.'), item )) as stream:
        try:
          conf[item] = yaml.load(stream)
        except yaml.YAMLError, e:
          print(e)
          sys.exit(1)
    except IOError, e:
      print(e)
      sys.exit(1)
    if not getenv('RAILS_ENV',default='development') in conf[item]:
      print('Error: configuration for env \''+getenv('RAILS_ENV')+'\' is not found in '+item+'.yml')
      sys.exit(1)
  setdefaulttimeout(get_conf('secrets','timeout'))
  if get_conf('secrets','debug'):
    imaplib.Debug = 4
  try:
    imapclient = imaplib.IMAP4_SSL(get_conf('secrets','imap').get('server'), get_conf('secrets','imap').get('port'))
  except (imaplib.IMAP4.error,SSLError), e:
    if 'message' in dir(e):
      print(e.message)
      syslog.syslog(e.message)
    else:
      print(e)
      syslog.syslog(e)
    sys.exit(1)

  dbconn = psycopg2.connect(
      database = get_conf('database','database'),
      user = get_conf('database','username'),
      password = get_conf('database','password'),
      host = get_conf('database','host'),
      port = get_conf('database','port')
    )
  dbcursor = dbconn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
  imapclient.login(get_conf('secrets','imap').get('login'), str(get_conf('secrets','imap').get('password')))

  imapclient.select('INBOX')
  rv,data= imapclient.search(None, "(UNSEEN)")
  if rv == "OK":
    for msg_id in data[0].split(' '):
      if msg_id:
        if get_conf('secrets','debug'):
          print('parsing msg_id=%s' % msg_id)
          syslog.syslog('parsing msg_id=%s' % msg_id)
        typ, data = imapclient.fetch(msg_id,'(RFC822)')
        message = Parser().parsestr(data[0][1])
        from_data = message.get('From')
        to_data = message.get('To')
        subject = message.get('Subject')
        if not parseaddr(to_data)[1]:
          continue
        dbcursor.execute('SELECT support_email FROM vendors WHERE kiosk_email=(%s)',(to_data,))
        if dbcursor.rowcount == 0:
          syslog.syslog ("kiosk_email=%s not found" % to_data)
          continue
        else:
          support_email = dbcursor.fetchone().support_email
          if support_email is None:
            syslog.syslog("support_email=none")
            continue
          else:
            mailer(message.as_string(),support_email)



if __name__ == "__main__":
  signal.signal(signal.SIGINT, sigint_handler)
  main()
