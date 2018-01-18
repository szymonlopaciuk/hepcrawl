# -*- coding: utf-8 -*-
#
# This file is part of hepcrawl.
# Copyright (C) 2015, 2016, 2017 CERN.
#
# hepcrawl is a free software; you can redistribute it and/or modify it
# under the terms of the Revised BSD License; see LICENSE file for
# more details.

"""Spider for Springer."""

from __future__ import absolute_import, division, print_function

from scrapy import Request
from scrapy.exceptions import CloseSpider
from scrapy.selector import Selector
from urllib import urlencode

from . import StatefulSpider
from ..parsers import JatsParser
from ..utils import ParsedItem


class SpringerSpider(StatefulSpider):
    """Springer crawler.

    Connects to the Springer API and fetches records in JATS.

    Example:
        $ scrapy crawl springer
    """

    name = 'springer'

    open_access_journals = {
        'Applied Network Science': '41109',
        'Cancer Convergence': '41236',
        'Computational Astrophysics and Cosmology': '40668',
        'EPJ Quantum Technology': '40507',
        'EPJ Techniques and Instrumentation': '40485',
        'Journal of the European Optical Society-Rapid Publications': '41476',
        'Journal of Theoretical and Applied Physics': '40094',
        'Photonic Sensors': '13320',
    }

    API_KEY = '622e91f592258e1dcc96458e42d0d0ba'

    def __init__(self, *args, **kwargs):
        """Construct Springer spider."""
        super(SpringerSpider, self).__init__(*args, **kwargs)

    def start_requests(self):
        journals = ' OR '.join(
            'journalid:{}'.format(id_)
            for id_ in self.open_access_journals.values()
        )
        return [
            self.construct_request(
                query='year:2017 AND sort:sequence AND ({})'.format(journals)
            )
        ]

    def construct_request(
        self,
        collection='openaccess',
        format='jats',
        query='',
        page_index=1,
        page_size=1,
    ):
        params = {
            'api_key': self.API_KEY,
            'q': query,
            's': page_index,
            'p': page_size
        }
        endpoint = 'http://api.springer.com/{}/{}'.format(collection, format)
        return Request(endpoint + '?' + urlencode(params))

    def parse(self, response):
        node = Selector(response, type='xml')
        result = node.xpath('/response/result')[0]
        self.logger.info(
            'Received page # {} of size {}, total records {}'.format(
                result.xpath('./start/text()').extract_first(),
                result.xpath('./pageLength/text()').extract_first(),
                result.xpath('./total/text()').extract_first(),
            )
        )

        for article in node.xpath('//article'):
            yield self.parse_node(response, article)

    def parse_node(self, response, node):
        self.logger.info('Parsing record...')
        parser = JatsParser(node, source='Springer')
        record = parser.parse()
        self.logger.info(
            'Record parsed successfully: {}'.format(record['dois'][0]['value'])
        )

        if self.has_been_parsed(parser.dois[0]):
            raise CloseSpider(
                'Record {} has already been harvested before! Stopping.'
                    .format(parser.dois[0])
            )

        # Could filter uninteresting articles here

        return ParsedItem(
            record=record,
            record_format='hep',
        )

    def has_been_parsed(self, doi):
        return False  # TODO: Implementation