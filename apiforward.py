#!/usr/bin/env python3
#
# Mail gateway for the Rossum Invoice Robot API.
#
# Usage: apiforward.py SECRETKEY [APIURL [BCC_DEBUG_ADDR]] <MAILFILE
#
# Example (/etc/aliases):
#   invoices: |/home/user/rir-api-email/apiforward.py BuFexzDBUBoL9kFkaATxrmyUkmpMi4wyag8Pzgh7iXmKnVYtUMpdD6cEpMHrsueq
# (N.B. to avoid potential secret key leak e.g. on timeout,
# using a shell script wrapper is recommended.)

from __future__ import print_function, division

import csv
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.parser import Parser
import io
import json
import requests
import smtplib
import sys
import time


# Do not process more than this number of attachments.
# With the default postfix command timeout of 1000s and conservatively
# assuming 60s per document, 15*60 = 900s still fits within the timeout.
MAX_ATTACHMENTS = 15


class RIRAPI(object):
    """ RIR API wrapper """
    def __init__(self, secret_key, url='https://us.api.rossum.ai'):
        self.secret_key = secret_key
        self.url = url
        self.headers = {'Authorization': 'secret_key ' + self.secret_key}

    def post_document(self, fp):
        """ Post a new document for processing; returns a dict with 'id' mapping to the job id. """
        r = requests.post(self.url + '/document', files={'file': fp}, headers=self.headers)
        # TODO retry on 5xx
        return json.loads(r.text)

    def poll_document(self, job_id):
        """ Wait until a document with given job id is processed, then return the results dict. """
        while True:
            r = requests.get(self.url + '/document/%s' % (job_id,), headers=self.headers)
            # TODO retry on 5xx
            res = json.loads(r.text)
            if res['status'] == 'processing':
                time.sleep(2)
                continue
            else:
                return res


def msg_to_api(msg, api):
    docs = []
    for part in msg.walk():
        if len(docs) >= MAX_ATTACHMENTS:
            break

        # multipart/* and message/* are just containers
        if part.get_content_maintype() in ['multipart', 'message']:
            continue
        # ignore standard body non-attachments
        if part.get_content_maintype() == 'text':
            continue

        # submit to the API
        doc_fp = io.BytesIO(part.get_payload(decode=True))
        time.sleep(1)
        doc = {'id': api.post_document(doc_fp)['id'], 'filename': part.get_filename()}
        print(doc['id'])
        docs.append(doc)

    for doc in docs:
        doc['r'] = api.poll_document(doc['id'])

    return docs


def results_to_csv(docs):
    fieldnames = ['filename', 'status', 'preview'] + sorted(set([f['name'] for doc in docs for f in doc['r'].get('fields', [])]))
    csv_fp = io.StringIO()
    writer = csv.DictWriter(csv_fp, fieldnames)
    writer.writeheader()
    for doc in docs:
        row = dict(filename=doc['filename'], status=doc['r']['status'], preview=doc['r'].get('preview', ''))
        for f in doc['r'].get('fields', []):
            row[f['name']] = f['content']
        writer.writerow(row)
    return csv_fp.getvalue()


def msg_reply(msg_in, csv_str):
    msg_out = MIMEMultipart()
    msg_out['From'] = 'Rossum Invoice Robot <support@rossum.ai>'
    msg_out['To'] = msg_in['reply-to'] if msg_in.get('reply-to', None) else msg_in['from']
    try:
        msg_out['Subject'] = 'Re: ' + msg_in['subject']
    except:
        msg_out['Subject'] = 'Re: (RIR Submission)'

    attachment = MIMEText(csv_str, _subtype='csv')
    csv_fname = time.strftime('%y%m%dT%H%M%S.csv')
    attachment.add_header('Content-Disposition', 'attachment', filename=csv_fname)
    msg_out.attach(attachment)
    return msg_out


if __name__ == "__main__":
    secret_key = sys.argv[1]
    try:
        api_url = sys.argv[2]
    except:
        api_url = 'https://us.api.rossum.ai'
    try:
        bcc = sys.argv[3]
    except:
        bcc = None

    api = RIRAPI(secret_key=secret_key, url=api_url)

    msg_raw = sys.stdin.read()
    msg = Parser().parsestr(msg_raw)

    docs = msg_to_api(msg, api)

    csv_str = results_to_csv(docs)

    msg_out = msg_reply(msg, csv_str)

    if True:
        s = smtplib.SMTP('localhost')
        s.sendmail(msg_out['From'], msg_out['To'], msg_out.as_string())
        if bcc is not None:
            s.sendmail(msg_out['From'], bcc, msg_out.as_string())
        s.quit()
    else:
        fp = open('msg.raw', 'w')
        fp.write(msg_out.as_string())
        fp.close()
