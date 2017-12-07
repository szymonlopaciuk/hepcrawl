# -*- coding: utf-8 -*-
#
# This file is part of hepcrawl.
# Copyright (C) 2016, 2017 CERN.
#
# hepcrawl is a free software; you can redistribute it and/or modify it
# under the terms of the Revised BSD License; see LICENSE file for
# more details.

"""Generaic spider for OAI-PMH."""

from __future__ import absolute_import, division, print_function

import logging
from sickle import Sickle

from scrapy import Request
from scrapy.spiders import Spider

from . import StatefulSpider

logger = logging.getLogger(__name__)

class OAISpider(Spider):
    """Spider for crawling OAI-PMH XML, to be subclassed.

    Example:
        Using OAI-PMH services::

            $ scrapy crawl OAI -a 'endpoint=http://export.arxiv.org/oai2'
                             \ -a 'format=oai_dc'
                             \ -a 'from_date=2016-05-01'
                             \ -a 'until_date=2016-05-15'
                             \ -a 'set=physics:hep-th'

    """

    name = 'OAI'

    def __init__(self, endpoint, from_date=None, until_date=None, oai_set=None,
                 format='oai_dc', **kwargs):
        """Construct OAI spider."""
        super(OAISpider, self).__init__(**kwargs)
        self.endpoint = endpoint
        params = {
            'from': from_date,
            'until': until_date,
            'set': oai_set,
            'metadataPrefix': format
        }
        self.oai_params = {
            param: value for param, value in params.items() if value
        }

    def start_requests(self):
        logger.info("Starting harvest with {}".format(repr(self.oai_params)))
        yield Request('oaipmh+{}'.format(self.endpoint))

    def parse(self, response):
        """Parse an OAI-PMH records."""
        sickle = Sickle(self.endpoint)
        records = sickle.ListRecords(**self.oai_params)
        for record in records:
            yield self.parse_record(records.oai_response.http_response, record)

    def parse_record(self, response, oai_record):
        """Parse a single OAI-PMH record, this method is to be overriden.

        Args:
            oai_record (sickle.models.Record): single record
            response (requests.models.Response): HTTP response
        """
        return oai_record.metadata