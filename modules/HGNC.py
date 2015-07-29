from collections import OrderedDict
import copy
from datetime import datetime
import logging
from StringIO import StringIO
import urllib2
from sqlalchemy import and_
import ujson as json
from common import Actions
from common.DataStructure import JSONSerializable
from common.ElasticsearchLoader import JSONObjectStorage
from common.PGAdapter import HgncInfoLookup
from settings import Config
from datetime import datetime

__author__ = 'gautierk'

class HGNCActions(Actions):
    UPLOAD='upload'

class HGNCUploader():

    def __init__(self, adapter):
        self.adapter = adapter
        self.session = adapter.session

    def upload(self):
        now = datetime.utcnow()
        today = datetime.strptime("{:%Y-%m-%d}".format(datetime.now()), '%Y-%m-%d')
        # Check this was not stored today already (we don't overwrite)
        count = self.session.query(HgncInfoLookup).filter_by(last_updated=today).count()
        if count == 0:
            # get a new version
            req = urllib2.Request('ftp://ftp.ebi.ac.uk/pub/databases/genenames/new/json/hgnc_complete_set.json')
            response = urllib2.urlopen(req)
            data = json.loads(response.read())
            #req.close()
            # now store it
            # http://docs.sqlalchemy.org/en/latest/dialects/postgresql.html
            hr = HgncInfoLookup(
                    last_updated = today,
                    data = data
                    )
            self.session.add(hr)
            self.session.commit()            
            logging.info("inserted hgnc_complete_set.json in JSONB format in the hgnc table for {:%d, %b %Y}".format(today))
        else:
            logging.info("already inserted hgnc_complete_set.json")            
