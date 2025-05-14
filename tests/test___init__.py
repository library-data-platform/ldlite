from unittest import TestCase

from ldlite import LDLite


class TestLDLite(TestCase):
    def test_connect_db(self):
        ld = LDLite()
        ld.connect_folio(url="https://folio-etesting-snapshot-kong.ci.folio.org",
                         tenant="diku",
                         user="diku_admin",
                         password="admin")
        _ = ld.connect_db()
        _ = ld.query(table="g", path="/groups", query="cql.allRecords=1 sortby id")
        ld.select(table="g__t")
