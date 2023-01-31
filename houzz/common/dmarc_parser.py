#!/usr/bin/env python

''' Based on: https://raw.github.com/thinkingserious/sendgrid-python-dmarc-parser/master/dmarc_parser/parse_dmarc.py

    Changes:
    - pylint suggestions
    - Decoupled DB inserts, converted functions to generators
'''

from __future__ import absolute_import
from __future__ import print_function
import xml.etree.ElementTree as ET

__author__ = 'Elmer Thomas'
__version__ = '0.1'

class DmarcParser:
    """Parse a DMARC XML formatted report"""
    def __init__(self, input_filename):
        """Attributes needed for file processing

        Keyword arguements:
        input_filename -- file that needs to be parsed
        doc -- parsed root of DOM object
        """
        self.doc = None
        self.input_filename = input_filename

    def parse(self):
        """Open the file to be Parsed and input the XML into the DOM"""
        dom = ET.parse(self.input_filename)
        self.doc = dom.getroot()

    def get_report_metadata(self):
        """Extract the DMARC metadata as defined here:
        http://www.dmarc.org/draft-dmarc-base-00-02.txt in Appendix C
        """
        orgName = self.doc.findtext("report_metadata/org_name", default="NA")
        email = self.doc.findtext("report_metadata/email", default="NA")
        extraContactInfo = self.doc.findtext("report_metadata/extra_contact_info", default="NA")
        reportID = self.doc.findtext("report_metadata/report_id", default="NA")
        dateRangeBegin = self.doc.findtext("report_metadata/date_range/begin", default="NA")
        dateRangeEnd = self.doc.findtext("report_metadata/date_range/end", default="NA")

        results = {
            'org_name': orgName,
            'email': email,
            'extra_contact_information': extraContactInfo,
            'report_id': reportID,
            'date_range_begin': dateRangeBegin,
            'date_range_end': dateRangeEnd,
        }
        return results


    def get_policy_published(self):
        """Extract the DMARC policy published information as defined here:
        http://www.dmarc.org/draft-dmarc-base-00-02.txt in Section 6.2
        """
        domain = self.doc.findtext("policy_published/domain", default="NA")
        adkim = self.doc.findtext("policy_published/adkim", default="NA")
        aspf = self.doc.findtext("policy_published/aspf", default="NA")
        p = self.doc.findtext("policy_published/p", default="NA")
        pct = self.doc.findtext("policy_published/pct", default="NA")
        results = {
            'policy_domain': domain,
            'adkim': adkim,
            'aspf': aspf,
            'p': p,
            'pct': pct,
        }
        return results


    def get_record(self):
        """Extract the DMARC records as defined here:
        http://www.dmarc.org/draft-dmarc-base-00-02.txt in Appendix C
        """
        container = self.doc.findall("record")
        for elem in container:
            source_ip = elem.findtext("row/source_ip", default="NA")
            count = elem.findtext("row/count", default="NA")
            count = int(count)
            disposition = elem.findtext("row/policy_evaluated/disposition", default="NA")
            dkim = elem.findtext("row/policy_evaluated/dkim", default="NA")
            spf = elem.findtext("row/policy_evaluated/spf", default="NA")
            reason_type = elem.findtext("row/policy_evaluated/reason/type", default="NA")
            comment = elem.findtext("row/policy_evaluated/reason/comment", default="NA")
            header_from = elem.findtext("identifiers/header_from", default="NA")
            dkim_domain = elem.findtext("auth_results/dkim/domain", default="NA")
            dkim_result = elem.findtext("auth_results/dkim/result", default="NA")
            dkim_hresult = elem.findtext("auth_results/dkim/human_result", default="NA")
            spf_domain = elem.findtext("auth_results/spf/domain", default="NA")
            spf_result = elem.findtext("auth_results/spf/result", default="NA")

            results = {
                'source_ip': source_ip,
                'count': count,
                'disposition': disposition,
                'dkim': dkim,
                'spf': spf,
                'type': reason_type,
                'comment': comment,
                'header_from': header_from,
                'dkim_domain': dkim_domain,
                'dkim_result': dkim_result,
                'dkim_hresult': dkim_hresult,
                'spf_domain': spf_domain,
                'spf_result': spf_result,
            }
            yield results


## main
if __name__ == '__main__':
    input_filename = '/Users/kenyung/houzz/c2/python/utils/dmarc_report_theartfarm.xml'
    parser = DmarcParser(input_filename)
    parser.parse()
    print(parser.get_report_metadata())
    print(parser.get_policy_published())
    for record in parser.get_record():
        print(record)
