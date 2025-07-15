import logging as log

from os import environ
from simple_salesforce import Salesforce
from string import Template


def run_salesforce_query(query: str) -> dict:
    """Returns a Dictionary generated from a Salesforce Session QueryAll"""

    salesforce_session = Salesforce(
        username=environ["USERNAME"],
        password=environ["PASSWORD"],
        security_token=environ["SECURITY_TOKEN"],
    )
    log.debug("Querying salesforce")
    query_response = dict(salesforce_session.query_all(query=query))
    
    return query_response


SALESFORCE_QUERY = Template(
    """
SELECT 
    Name, 
    StageName, 
    ForecastCategoryName, 
    Comms_vs_Identity__c, 
    Sales_Team_Region__c, 
    CloseDate, 
    Amount_Direct_Margin__c, 
    CreatedDate, 
    SAO_Date__c 
FROM Opportunity 
WHERE 
    IsSalesRecordType__c = True 
    AND (CloseDate >= $MIN_DATE OR CreatedDate >= $MIN_DATETIME)
"""
)
