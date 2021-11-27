from unittest import TestCase

from src.ldlite import *


class TestLDLite(TestCase):
    def test_connect_db(self):
        ld = LDLite()
        ld.connect_okapi(url='https://folio-juniper-okapi.dev.folio.org/',
                         tenant='diku',
                         user='diku_admin',
                         password='admin')
        _ = ld.connect_db()
        _ = ld.query(table='g', path='/groups', query='cql.allRecords=1 sortby id')
        ld.select(table='g__t')

    # def test_connect_okapi(self):
    #     self.fail()
    #
    # def test_query(self):
    #     self.fail()
    #
    # def test_select(self):
    #     self.fail()
