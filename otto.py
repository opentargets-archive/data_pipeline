#!/usr/bin/env python

from cement.core.foundation import CementApp
from cement.ext.ext_argparse import ArgparseController, expose

class OTTOBaseController(ArgparseController):
    class Meta:
        label = 'base'
        description = "OTTO - OpenTargets TOolbox - tools to process data for opentargets"
        extensions = ['colorlog']
        log_handler = 'colorlog'
        arguments_override_config = True
        arguments = [
            ( ['-e', '--elasticsearch'],
              dict(action='store', help='url for elasticsearch host. E.g. http://localhost:9200') ),
            ]


    @expose(hide=True)
    def default(self):
        self.app.log.info('Inside OTTOBaseController.default()')
        # if self.app.pargs.foo:
        #     print("Recieved option: foo => %s" % self.app.pargs.foo)


class FetchController(ArgparseController):
    class Meta:
        label = 'fetch'
        stacked_on = 'base'
        stacked_type = 'nested'
        description = "fetches data from public databases"
        arguments = [
            (['--postgres'], dict(help="postgres host")),
        ]

    @expose(help="fetch default command", hide=True)
    def default(self):
        print "please specify a db"

    @expose(help="fetch data from uniprot")
    def uniprot(self):
        self.app.log.info("fetching uniprot data")

    @expose(help="fetch data from ensembl")
    def ensembl(self):
        self.app.log.info("fetching ensembl data")

    @expose(help="fetch data from reactome")
    def reactome(self):
        self.app.log.info("fetching reactome data")

    @expose(help="fetch data from human protein atlas")
    def hpa(self):
        self.app.log.info("fetching human protein atlas data")

    @expose(help="fetch data from EFO")
    def efo(self):
        self.app.log.info("fetching EFO data")

    @expose(help="fetch data from ECO")
    def eco(self):
        self.app.log.info("fetching ECO data")

class LoadController(ArgparseController):
    class Meta:
        label = 'load'
        stacked_on = 'base'
        stacked_type = 'nested'
        description = "load evidence data in opentargets datastore"
        arguments = [
            (['--2nd-opt'], dict(help="another option under base controller")),
        ]

    @expose(help="load default command", hide=True)
    def default(self):
        print "please specify a file"

    @expose(help="loads data form local file")
    def local(self):
        self.app.log.info("loading local file data")

    @expose(help="loads data form remote sftp")
    def sftp(self):
        self.app.log.info("loading remote file over sftp")

class ProcessController(ArgparseController):
    class Meta:
        label = 'process'
        stacked_on = 'base'
        stacked_type = 'nested'
        description = "processing steps in data in opentargets data pipeline"
        arguments = [
            (['--2nd-opt'], dict(help="another option under base controller")),
        ]

    @expose(help="process default command", hide=True)
    def default(self):
        print "please specify a processing step"

    @expose(help="process evidence data")
    def evidence(self):
        self.app.log.info("processing evidence data")

    @expose(help="process association data")
    def association(self):
        self.app.log.info("processing association data")

    @expose(help="process search data")
    def search(self):
        self.app.log.info("processing search data")

class QAController(ArgparseController):
    class Meta:
        label = 'qa'
        stacked_on = 'base'
        stacked_type = 'nested'
        description = "assess quality of data"
        arguments = [
            (['--2nd-opt'], dict(help="another option under base controller")),
        ]

    @expose(help="qa default command", hide=True)
    def default(self):
        print "please specify a qa step"

    @expose(help="assess association data quality")
    def association(self):
        self.app.log.info("processing evidence data")

class DumpController(ArgparseController):
    class Meta:
        label = 'dump'
        stacked_on = 'base'
        stacked_type = 'nested'
        description = "dump processed data"
        arguments = [
            (['--2nd-opt'], dict(help="another option under base controller")),
        ]

    @expose(help="dump default command", hide=True)
    def default(self):
        print "please specify a dump step"

    @expose(help="dump evidence ")
    def evidence(self):
        self.app.log.info("dumping evidence data")

    @expose(help="dump association ")
    def association(self):
        self.app.log.info("dumping association data")


class OTTOApp(CementApp):
    class Meta:
        label = 'OTTO'
        base_controller = 'base'
        # extensions = ['argcomplete']
        handlers = [OTTOBaseController,
                    FetchController,
                    LoadController,
                    ProcessController,
                    QAController,
                    DumpController,
                    ]



with OTTOApp() as app:
    app.run()