
import logging
import time

class ElasticsearchBulkIndexManager(object):
    def __init__(self, client, index_name, settings = {}, mappings = {}):
        """
        client is an elasticsearch client object
        """
        self.logger = logging.getLogger(__name__)
        self.client = client
        self.index_name = index_name
        #these are set on entry 
        self.old_number_of_replicas = None
        self.old_refresh_interval = None
        self.old_translog_durability = None
        #these might or might not be set
        self.settings = settings
        self.mappings = mappings

    def __enter__(self):
        #setup
        #ensure the index exists and is empty and ready
        #ignore if index doesn't exist
        if self.client.indices.exists(index=self.index_name):
            self.logger.debug("deleting prevous index %s", self.index_name)
            self.client.indices.delete(index=self.index_name, ignore=[404])
        self.logger.debug("creating index %s", self.index_name)
        
        body = {
            "settings": self.settings,
            "mappings": self.mappings
        }
        self.client.indices.create(index=self.index_name, body=body)

        #store old settings to restore later, if present
        self.logger.debug("saving old settings for %s", self.index_name)
        old_settings = self.client.indices.get_settings(self.index_name)
        if self.index_name in old_settings:
            if "settings" in old_settings[self.index_name]:
                if "index" in old_settings[self.index_name]["settings"]:
                    if "number_of_replicas" in old_settings[self.index_name]["settings"]["index"]:
                        #store number of replicas
                        self.old_number_of_replicas = old_settings[self.index_name]["settings"]["index"]["number_of_replicas"]
                    if "refresh_interval" in old_settings[self.index_name]["settings"]["index"]:
                        #store index interval
                        self.old_refresh_interval = old_settings[self.index_name]["settings"]["index"]["refresh_interval"]
                    if "translog.durability" in old_settings[self.index_name]["settings"]["index"]:
                        #store transaction log durability setting
                        self.old_translog_durability = old_settings[self.index_name]["settings"]["index"]["translog.durability"]


        #set replicas to zero
        #set update interval to "never"
        #set transaction log durability to "async"
        self.logger.debug("changing settings for bulk into %s", self.index_name)
        self.client.indices.put_settings(index=self.index_name, body={
            "index" : {
                "number_of_replicas" : 0,
                "refresh_interval" : -1,
                "translog.durability" : "async"
            }
        })
        return self
        
    def __exit__(self, type, value, traceback):
        #teardown

        #restore old settings
        self.logger.debug("Restoring old settings for %s", self.index_name)
        self.client.indices.put_settings(index=self.index_name, body={
            "index" : {
                "number_of_replicas" : self.old_number_of_replicas,
                "refresh_interval" : self.old_refresh_interval,
                "translog.durability" : self.old_translog_durability
            }
        })
        

        #run force-merge
        #this will compress everyhting into a single "segment"
        #temporarily, will use more disk as things are copied around
        #but in the end should be smaller and more performant
        self.logger.debug("Force merging %s", self.index_name)
        self.client.indices.forcemerge(index=self.index_name)

        #wait for everthing to sort itself out
        self.wait_for_green()

        #don't return True to indicate any exceptions have been handled
        #this contex manager is only for cleanup
        return None

    def wait_for_green(self):
        #TODO implement a timeout?
        self.logger.debug("Checking index status %s", self.index_name)
        status = None
        while status != u"green":
            time.sleep(1)
            status = self.client.cat.indices(index=self.index_name).strip().split()[0]
            self.logger.debug("Status of %s is %s", self.index_name, status)
