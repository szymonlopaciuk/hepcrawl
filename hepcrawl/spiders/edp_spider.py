# -*- coding: utf-8 -*-
#
# This file is part of hepcrawl.
# Copyright (C) 2016, 2017 CERN.
#
# hepcrawl is a free software; you can redistribute it and/or modify it
# under the terms of the Revised BSD License; see LICENSE file for
# more details.

"""Spider for EDP Sciences."""

from __future__ import absolute_import, division, print_function

import os
import urlparse
import tarfile
from tempfile import mkdtemp

from inspire_schemas.builders import LiteratureBuilder
from inspire_utils.record import get_value
from scrapy import Request
from scrapy.spiders import XMLFeedSpider

from . import StatefulSpider
from ..parsers.jats import JatsParser
from ..items import HEPRecord
from ..loaders import HEPLoader
from ..utils import (
    ftp_list_files,
    ftp_connection_info,
    get_first,
    get_node,
    parse_domain,
    ParsedItem,
    strict_kwargs,
)


class EDPSpider(StatefulSpider, XMLFeedSpider):
    """EDP Sciences crawler.

    This spider connects to a given FTP hosts and downloads zip files with
    XML files for extraction into HEP records.

    This means that it generates the URLs for Scrapy to crawl in a special way:

    1. First it connects to a FTP host and lists all the new TAR files found
       on the remote server and downloads them to a designated local folder,
       using ``EDPSpider.start_requests()``. The starting point of the crawl
       can also be a local file. Packages contain XML files with different
       formats (``gz`` package is ``JATS``, ``bz2`` package has ``rich`` and
       ``jp`` format XML files, ``jp`` is ``JATS``.)

    2. Then the TAR file is unpacked and it lists all the XML files found
       inside, via ``EDPSpider.handle_package()``. Note the callback from
       ``EDPSpider.start_requests()``.

    3. Each XML file is parsed via ``EDPSpider.parse_node()``.

    4. PDF file is fetched from the web page in ``EDPSpider.scrape_for_pdf()``.

    5. Finally, ``HEPRecord`` is created in ``EDPSpider.build_item()``.

    Examples:
        To run an ``EDPSpider``, you need to pass FTP connection information
        via ``ftp_netrc`` file::

            $ scrapy crawl EDP -a ftp_netrc=tmp/edps_netrc

        To run an ``EDPSpider`` using ``rich`` format::

            $ scrapy crawl EDP -a source_folder=file://`pwd`/tests/responses/edp/test_rich.tar.bz2

        To run an ``EDPSpider`` using ``gz`` format::

            $ scrapy crawl EDP -a source_folder=file://`pwd`/tests/responses/edp/test_gz.tar.gz

    Todo:

     Sometimes there are errors:

        .. code-block:: python

            Unhandled Error
            self.f.seek(-size-self.SIZE_SIZE, os.SEEK_END)
            exceptions.IOError: [Errno 22] Invalid argument

        OR

        .. code-block:: python

            ConnectionLost: ('FTP connection lost',
            <twisted.python.failure.Failure twisted.internet.error.ConnectionDone:
            Connection was closed cleanly.>)

        See old harvesting-kit:

            * https://github.com/inspirehep/harvesting-kit/blob/master/harvestingkit/edpsciences_package.py
            * https://github.com/inspirehep/inspire/blob/master/bibtasklets/bst_edpsciences_harvest.py
    """
    name = 'EDP'
    custom_settings = {}
    start_urls = []
    iterator = 'xml'
    itertag = 'article'
    download_delay = 10
    custom_settings = {'MAX_CONCURRENT_REQUESTS_PER_DOMAIN': 2}

    allowed_article_types = [
        'research-article',
        'corrected-article',
        'original-article',
        'introduction',
        'letter',
        'correction',
        'addendum',
        'review-article',
        'rapid-communications',
        'Article',
        'Erratum',
    ]

    OPEN_ACCESS_JOURNALS = {
        'EPJ Web of Conferences'
    }

    @strict_kwargs
    def __init__(self, package_path=None, ftp_folder="incoming", ftp_netrc=None, *args, **kwargs):
        """Construct EDP spider.

        :param package_path: path to local tar.gz or tar.bz2 package.
        :param ftp_folder: path on remote ftp server.
        :param ftp_netrc: path to netrc file.
        """
        super(EDPSpider, self).__init__(*args, **kwargs)
        self.ftp_folder = ftp_folder
        self.ftp_host = "ftp.edpsciences.org"
        self.ftp_netrc = ftp_netrc
        self.target_folder = mkdtemp(prefix='EDP_', dir='/tmp/')
        self.package_path = package_path
        if not os.path.exists(self.target_folder):
            os.makedirs(self.target_folder)

    def start_requests(self):
        """List selected folder on remote FTP and yield new zip files."""
        if self.package_path:
            yield Request(self.package_path, callback=self.handle_package_file)
        else:
            ftp_host, ftp_params = ftp_connection_info(
                self.ftp_host, self.ftp_netrc)
            _, new_files = ftp_list_files(
                server_folder=self.ftp_folder,
                destination_folder=self.target_folder,
                ftp_host=ftp_host,
                user=ftp_params['ftp_user'],
                password=ftp_params['ftp_password']
            )
            for remote_file in new_files:
                # Cast to byte-string for scrapy compatibility
                remote_file = str(remote_file)
                ftp_params["ftp_local_filename"] = os.path.join(
                    self.target_folder,
                    os.path.basename(remote_file)
                )
                remote_url = "ftp://{0}/{1}".format(ftp_host, remote_file)
                yield Request(
                    str(remote_url),
                    meta=ftp_params,
                    callback=self.handle_package_ftp
                )

    def handle_package_ftp(self, response):
        """Handle remote packages and yield every XML found."""
        self.logger.info("Visited %s" % response.url)
        zip_filepath = response.body
        zip_target_folder, _ = os.path.splitext(zip_filepath)
        if "tar" in zip_target_folder:
            zip_target_folder, _ = os.path.splitext(zip_target_folder)
        xml_files = self.untar_files(zip_filepath, zip_target_folder)
        for xml_file in xml_files:
            yield Request(
                "file://{0}".format(xml_file),
                meta={"source_folder": zip_filepath}
            )

    def handle_package_file(self, response):
        """Handle a local package and yield every XML found."""
        zip_filepath = urlparse.urlsplit(response.url).path
        zip_target_folder, _ = os.path.splitext(zip_filepath)
        if "tar" in zip_target_folder:
            zip_target_folder, _ = os.path.splitext(zip_target_folder)
        xml_files = self.untar_files(zip_filepath, zip_target_folder)
        for xml_file in xml_files:
            request = Request(
                "file://{0}".format(xml_file),
                meta={"source_folder": zip_filepath}
            )
            if "xml_rich" in xml_file:
                request.meta["rich"] = True
                self.itertag = "EDPSArticle"
            yield request

    @staticmethod
    def untar_files(zip_filepath, target_folder, flatten=False):
        """Unpack the tar.gz or tar.bz2 package and return XML file paths."""
        xml_files = []
        with tarfile.open(zip_filepath) as tar:
            for filename in tar.getmembers():
                if filename.path.endswith(".xml"):
                    if flatten:
                        filename.name = os.path.basename(filename.name)
                    absolute_path = os.path.join(target_folder, filename.path)
                    if not os.path.exists(absolute_path):
                        tar.extract(filename, path=target_folder)
                    xml_files.append(absolute_path)

        return xml_files

    def parse_node(self, response, node):
        """Parse the XML file and yield a request to scrape for the PDF."""
        node.remove_namespaces()
        # Deal with open access restrictions and allowed article types
        if response.meta.get("rich"):
            item = self.build_item_rich(response, node)
            dois = item.record.get('dois', [])
            doi = dois[0] if len(dois) else None
            journal_title = item.record.get('journal_title')
        else:
            # import pdb
            # pdb.set_trace()
            item = self.build_item_jats(response, node)
            doi = get_value(item.record, 'dois.value[0]')
            journal_title = get_value(item.record, 'publication_info.journal_title[0]')

        if doi and journal_title in self.OPEN_ACCESS_JOURNALS:
            return Request(
                "http://dx.doi.org/" + doi,
                callback=self.scrape_for_pdf,
                meta={'parsed_item': item}
            )
        else:
            return item

    def scrape_for_pdf(self, response):
        """Try to find the fulltext pdf from the web page."""
        parsed_item = response.meta["parsed_item"]

        all_links = response.xpath(
            '//a[contains(@href, "pdf")]/@href'
        ).extract()
        domain = parse_domain(response.url)
        pdf_links = sorted(set(
            [urlparse.urljoin(domain, link) for link in all_links]
        ))

        self._attach_pdfs(parsed_item, pdf_links)
        self._attach_url(parsed_item, response.url)
        return parsed_item

    @classmethod
    def _attach_pdfs(self, item, pdf_urls):
        if item.record_format == 'hep':
            builder = LiteratureBuilder(record=item.record, source=self.source)

            for pdf_url in pdf_urls:
                builder.add_document(
                    key=os.path.basename(pdf_url),
                    url=pdf_url,
                    fulltext=True,
                    hidden=False,
                )
        elif item.record_format == 'hepcrawl':
            # NOTE: maybe this should be removed as the 'rich' format records
            # are not open access.
            item.record.add_value(
                "documents",
                self._create_file(
                    get_first(pdf_urls or []),
                    "INSPIRE-PUBLIC",
                    "Fulltext",
                    "pdf",
                )
            )

    @classmethod
    def _attach_url(self, item, url):
        if item.record_format == 'hep':
            builder = LiteratureBuilder(record=item.record, source=self.source)
            builder.add_url(url)
        elif item.record_format == 'hepcrawl':
            item.record.add_value("urls", [url])

    def build_item_rich(self, response, node):
        """Build the final HEPRecord with "rich" format XML."""
        article_type = response.meta.get("article_type")
        record = HEPLoader(item=HEPRecord(), selector=node, response=response)

        record.add_xpath('dois', './/DOI/text()')
        record.add_xpath('abstract', './/Abstract')
        record.add_xpath('title', './/ArticleTitle/Title')
        record.add_xpath('subtitle', './/ArticleTitle/Subtitle')
        record.add_value('authors', self._get_authors_rich(node))
        record.add_xpath('free_keywords', './/Subject/Keyword/text()')

        journal_title = node.xpath('.//JournalShortTitle/text()|//JournalTitle/text()').extract_first()
        record.add_value('journal_title', journal_title)
        record.add_xpath('journal_issue', './/Issue/text()')
        record.add_xpath('journal_volume', './/Volume/text()')
        fpage = node.xpath('.//FirstPage/text()').extract_first()
        lpage = node.xpath('.//LastPage/text()').extract_first()
        record.add_value('journal_fpage', fpage)
        record.add_value('journal_lpage', lpage)
        if fpage and lpage:
            record.add_value('page_nr', str(int(lpage) - int(fpage) + 1))

        journal_year = node.xpath('.//IssueID/Year/text()').extract()
        if journal_year:
            record.add_value('journal_year', int(journal_year[0]))
        record.add_value('date_published', self._get_date_published_rich(node))

        record.add_xpath('copyright_holder', './/Copyright/text()')
        record.add_value('collections', self._get_collections(
            node, article_type, journal_title))

        parsed_item = ParsedItem(
            record=record.load_item(),
            record_format='hepcrawl',
        )

        return parsed_item

    def build_item_jats(self, response, node):
        """Build the final HEPRecord with JATS-format XML ('jp')."""
        parser = JatsParser(node, source="EDP")

        parsed_item = ParsedItem(
            record=parser.parse(),
            record_format='hep',
        )

        return parsed_item

    def _get_date_published_rich(self, node):
        """Get published date."""
        date_published = ""
        year = node.xpath('.//Year/text()').extract_first()
        month = node.xpath('.//MonthNumber/text()').extract_first()
        if year:
            date_published = year
            if month:
                date_published += "-" + month
        return date_published

    def _get_collections(self, node, article_type, current_journal_title):
        """Return this articles' collection."""
        conference = node.xpath('.//conference').extract()
        if conference or current_journal_title == "International Journal of Modern Physics: Conference Series":
            return ['HEP', 'ConferencePaper']
        elif article_type == "review-article":
            return ['HEP', 'Review']
        else:
            return ['HEP', 'Published']

    def _get_authors_rich(self, node):
        """Get authors and return formatted dictionary."""
        authors = []
        for contrib in node.xpath('.//Author'):
            surname = contrib.xpath(
                'AuthorName//LastName/text()').extract_first()
            fname = contrib.xpath(
                'AuthorName//FirstName/text()').extract_first()
            mname = contrib.xpath(
                'AuthorName//MiddleName/text()').extract_first()
            given_names = ""
            if fname:
                given_names = fname
                if mname:
                    given_names += " " + mname
            affiliations = []
            reffered_id = contrib.xpath('AffiliationID/@Label').extract_first()
            if reffered_id:
                aff_raw = node.xpath(
                    './/Affiliation[@ID="{0}"]/UnstructuredAffiliation/text()'.format(reffered_id)).extract()
            if aff_raw:
                affiliations = [{'value': aff} for aff in aff_raw]
            authors.append({
                'surname': surname,
                'given_names': given_names,
                'affiliations': affiliations,
            })

        return authors

    def _create_file(self, file_path, file_access, file_type):
        """Create a structured dictionary and add to 'files' item."""
        file_dict = {
            "access": file_access,
            "description": self.name.upper(),
            "url": file_path,
            "type": file_type,
        }
        return file_dict
