# This script uses LDLite to extract sample data from the FOLIO demo sites.

import sys
import ldlite
import time
import threading
import os
from dotenv import load_dotenv



# Demo sites
current_release = 'https://folio-lotus-okapi.dev.folio.org/'
latest_snapshot = 'https://folio-snapshot-okapi.dev.folio.org/'

username = os.getenv("OKAPI_USERNAME")
password = os.getenv("OKAPI_PASSWORD")

###############################################################################
# Select a demo site here:
selected_site = current_release
###############################################################################
# Note that these demo sites are unavailable at certain times in the evening
# (Eastern time) or if a bug is introduced and makes one of them unresponsive.
# At the time of this writing, the "current release" demo site appears to be
# more stable than the "latest snapshot" site.  For information about the
# status of the demo sites, please see the #hosted-reference-envs channel in
# the FOLIO Slack organization.  For general information about FOLIO demo
# sites, see the "Demo Sites" section of the FOLIO Wiki at:
# https://wiki.folio.org
###############################################################################

ld = ldlite.LDLite()
ld.connect_okapi(url=selected_site, tenant='diku', user='diku_admin', password='admin')

db = ld.connect_db(filename='ldlite.db')
# For PostgreSQL, use connect_db_postgresql() instead of connect_db():
# db = ld.connect_db_postgresql(dsn='dbname=ldlite host=localhost user=ldlite')

allrec = ldlite.allrec
queries = ldlite.queries

processQuery = ldlite._process_query(sys,ld,queries)

print('Tables:')
for t in tables:
    print(t)
print('(' + str(len(tables)) + ' tables)')

def signal_state():
    while True:
        time.sleep(5)
        print("getting token")
        x = ldlite.okapi_auth(url='https://okapi-fivecolleges.folio.ebsco.com/', username = username, password= password, tenant='fs00001006')
        ldlite.okapi_auth_refresh(url='https://okapi-fivecolleges.folio.ebsco.com/', username = username, password= password, tenant='fs00001006')
        print(x.token, x.expiry)
        event.set()
        time.sleep(10)
        print("time to renew token")
        event.clear()
def file_work():
    while True:
        print("Waiting for token")
        event.wait()
        print("token is ready")
        while event.is_set():
            processQuery
            time.sleep(2)
        print("time to renew token, pause to read file")

event= threading.Event()
t1= threading.Thread(target=signal_state)
t2= threading.Thread(target=file_work)
t1.start()
t2.start()