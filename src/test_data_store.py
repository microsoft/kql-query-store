# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------
"""Module docstring."""
import json
import random
import uuid
from pathlib import Path

import pytest
import pytest_check as check

from .kql_query import KqlQuery
from .data_store import DataStore
from .kql_extract import extract_kql

__author__ = "Ian Hellen"

# pylint: disable=redefined-outer-name

json_query_data = """
{
    "query_id": "1234291720927310",
    "source_path": "/github.com/foo",
    "source_type": "text",
    "source_index": 0,
    "name": "query_1",
    "query": "SecurityAlert\\n| Where foo == bar",
    "context": "text from markdown",
    "attributes": {
        "description": "Query one description",
        "tactics": ["Exploitation", "Compromise"],
        "techniques": ["T.1055", "T.1345"]
    }
}
"""

json_kql_parse = """
{
    "FunctionCalls":["count","tostring","make_list","toreal"],
    "Joins":["rightsemi","leftouter"],
    "Operators":["where","extend","summarize","mv-expand","project-away","project"],
    "Tables":["SigninLogs"]
}
"""

table_names = ['AADB2CRequestLogs', 'AADDomainServicesAccountLogon',
       'AADDomainServicesAccountManagement',
       'AADDomainServicesDirectoryServiceAccess',
       'AADDomainServicesLogonLogoff', 'AADDomainServicesPolicyChange',
       'AADDomainServicesPrivilegeUse', 'AADManagedIdentitySignInLogs',
       'AADNonInteractiveUserSignInLogs', 'AADProvisioningLogs',
       'AADRiskyServicePrincipals', 'AADRiskyUsers',
       'AADServicePrincipalRiskEvents', 'AADServicePrincipalSignInLogs',
       'AADUserRiskEvents', 'ADFSSignInLogs', 'AlertEvidence',
       'Anomalies', 'AppServiceIPSecAuditLogs',
       'AppServiceServerlessSecurityPluginData', 'ASimDnsActivityLogs',
       'AuditLogs', 'AWSCloudTrail', 'AWSGuardDuty', 'AWSVPCFlow',
       'AZFWApplicationRule', 'AZFWApplicationRuleAggregation',
       'AZFWDnsQuery', 'AZFWIdpsSignature',
       'AZFWInternalFqdnResolutionFailure', 'AZFWNatRule',
       'AZFWNatRuleAggregation', 'AZFWNetworkRule',
       'AZFWNetworkRuleAggregation', 'AZFWThreatIntel', 'AzureActivity',
       'AzureDiagnostics', 'BehaviorAnalytics', 'CloudAppEvents',
       'CommonSecurityLog', 'ConfidentialWatchlist', 'DeviceEvents',
       'DeviceFileCertificateInfo', 'DeviceFileEvents',
       'DeviceImageLoadEvents', 'DeviceInfo', 'DeviceLogonEvents',
       'DeviceNetworkEvents', 'DeviceNetworkInfo', 'DeviceProcessEvents',
       'DeviceRegistryEvents', 'DeviceTvmSecureConfigurationAssessment',
       'DeviceTvmSoftwareInventory', 'DeviceTvmSoftwareVulnerabilities',
       'DSMAzureBlobStorageLogs', 'DSMDataClassificationLogs',
       'DSMDataLabelingLogs', 'DynamicEventCollection',
       'EmailAttachmentInfo', 'EmailEvents', 'EmailPostDeliveryEvents',
       'EmailUrlInfo', 'GCPAuditLogs', 'HDInsightSecurityLogs',
       'HuntingBookmark', 'IdentityDirectoryEvents',
       'IdentityLogonEvents', 'IdentityQueryEvents', 'LinuxAuditLog',
       'McasShadowItReporting', 'NetworkAccessTraffic', 'NetworkSessions',
       'NSPAccessLogs', 'OfficeActivity', 'PowerBIActivity',
       'ProjectActivity', 'ProtectionStatus',
       'PurviewDataSensitivityLogs', 'SecurityAlert', 'SecurityBaseline',
       'SecurityBaselineSummary', 'SecurityDetection', 'SecurityEvent',
       'SecurityIoTRawEvent', 'SecurityRecommendation', 'SentinelAudit',
       'SentinelHealth', 'SigninLogs', 'Syslog',
       'ThreatIntelligenceIndicator', 'Update', 'UrlClickEvents',
       'UserAccessAnalytics', 'UserPeerAnalytics', 'Watchlist',
       'WindowsEvent', 'WindowsFirewall', 'WireData']

field_names = ['SourceType', 'DomainBehaviorVersion', 'OperationName',
       'BookmarkName', 'SentinelResourceId', 'OSName', 'ActualResult',
       'CreatedBy', 'CreatedDateTime', 'LatencySamplingTimeStamp',
       'Environment', 'CorrelationId', 'MachineGroup',
       'SumResponseBodySize', 'RecordId', 'DstUserUpn', 'ResourceId',
       'InitiatingProcessSHA1', 'ObjectId', 'AssetType', 'Title',
       'InitiatingProcessAccountDomain', 'AuthorizationInfo',
       'TargetContextId', 'LogonId', 'CveTags', 'SourceComputerId',
       'ResourceIdentity', 'ClusterName', 'TdoAttributes',
       'EntityMapping', 'DnssecOkBit', 'DeviceCustomString5',
       'TransmittedServices', 'DeviceCustomDate2Label']


def get_random_items(data=table_names, count=3):
    return list({
        random.choice(data)
        for _ in range(count)
    })

def get_random_query(index=0):
    tactic_idx = index % 7
    return {
        "query_id": str(uuid.uuid4()),
        "source_path": f"/github.com/foo/{index}",
        "source_type": "text",
        "source_index": random.randint(0, 7),
        "query_name": f"query_{index}",
        "query": "SecurityAlert\\n| Where foo == bar",
        # "context": "text from markdown",
        "attributes": {
            "description": "Query one description",
            "tactics": get_random_items(data=["Exploitation", "Compromise", "LateralMovement"], count=2),
            "techniques": [f"T10{tactic_idx:0>2d}", f"T1{tactic_idx:0>2d}5"],
            "test_dict": {"joins": {"inner": ["one", "two"], "outer": ["three", "four"]}}
        }
    }

@pytest.fixture
def get_raw_queries():

    return [get_random_query(i) for i in range(5)]

@pytest.fixture
def get_kqlquery_list():
    return [KqlQuery(**get_random_query(i)) for i in range(5)]


def test_datastore_init(get_kqlquery_list, get_raw_queries):

    ds = DataStore(get_kqlquery_list)
    all_items_len = len(get_kqlquery_list)
    assert len(ds._data) == all_items_len
    assert len(ds._data) == all_items_len
    assert len(ds._indexes) == 2

    ds = DataStore(get_raw_queries)
    all_items_len = len(get_kqlquery_list)
    assert len(ds._data) == all_items_len
    assert len(ds._data) == all_items_len
    assert len(ds._indexes) == 2

    json_text = ds.to_json()
    output_dict = json.loads(json_text)
    assert len(output_dict) == len(get_raw_queries)

    out_df = ds.to_df()
    assert len(out_df) == all_items_len


def test_datastore_find(get_kqlquery_list):

    ds = DataStore(get_kqlquery_list)
    all_items_len = len(get_kqlquery_list)
    assert len(ds.find_queries(query_name="query_0")) == 1
    assert all_items_len > len(ds.find_queries(tactics=["Compromise"]))
    assert len(ds.find_queries(tactics=["BadTactic"])) == 0
    assert len(ds.find_queries(query_name={"matches": "query.*"})) == all_items_len


_TEST_KQL = Path(__file__).parent.joinpath("kqlextraction/tests")

@pytest.fixture
def get_queries_with_kql():
    queries = []
    for file in Path(_TEST_KQL).glob("*.kql"):

        query_text = file.read_text(encoding="utf-8")
        for query in [KqlQuery(**get_random_query(i)) for i in range(2)]:
            query.query = query_text
            queries.append(query)
    return queries
