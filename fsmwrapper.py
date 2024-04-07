from oauth.datalakeoauth import new_oauth_object
from utilities.utilities import read_lines, count_lines
import threading
import json, urllib.parse
import requests as r

class FSMEndpoints:
    _BASE    = 'https://mingle35-ionapi.inforgov.com/{tenant_name}/FSM/fsm/soap'

    def __init__(self, tenant_name):
        self._BASE = self._BASE.format(tenant_name=tenant_name)
        self.GENERIC_LIST = self._BASE + '/classes/{bc_name}/lists/_generic?_fields={fields}&_limit={limit}'

class EndpointFilter:
    def __init__(self):
        pass

class DataExtractor:
    def __init__(self):
        pass

    def get(self):
        pass

class APIExtractor(DataExtractor):
    def __init__(self, oauth_request, endpoints):
        self.oauth_request = oauth_request
        self.endpoints = endpoints

    def get(self, url):
        response = r.get(
            url,
            headers = {'Authorization': f'Bearer {self.oauth_request.oauth_token.access_token}'}    
        )
       
        return response

def get_next_url(response):
    return f"{endpoints._BASE}/classes/{bc_name}/{response['_links'][1]['href'].replace('../', '')}"

def get_and_write_data(url, filename):
    write_to_file(data=url+'\n', filename=f'{filename}_urls.csv', encoding='utf-8')
    response = data_extractor.get(
        url=url
    )

    if not response.ok:
        return (None, None, None)

    header, data = parse_response(response)
    if not header or header['_count'] == 0:
        print('Reached end of record set.')
        return (None, None, None)
    
    write_to_file(data=f"{header['_count']}"+'\n', filename=f'{filename}_counts.csv', encoding='utf-8')    

    schema, data = parse_records((header, data))

    def write_schema():
        write_to_file(schema+'\n', f'{filename}.csv')

    def write_data():
        write_to_file('\n'.join(data) + '\n', f'{filename}.csv')

    return (write_schema, write_data, get_next_url(header))

def get_api_header(url, bc_name, agency):
    response = data_extractor.get(
        url=url
    )

    if not response.ok:
        return (None, None, None)

    to_dict = response.content.decode('utf-8')
    records = json.loads(to_dict)

    header = records.pop(0)

    # with open(f'fsm-data/GLTransactionDetail_{agency}_counts.csv', 'r') as f:
    #     total = [t.strip() for t in f.readlines()][-1]
    
    @lock_resource
    def write():
        with open(f'fsm-data/GLTransactionDetail_{agency}_recounts.csv', 'w') as f:
            f.write(
                f"{header['_count']}\n"
            )

    write()

def get_data_loop(url, filename):
    print(f'Thread id: {threading.currentThread().ident} looping for data.')

    write_schema, write_data, next_url = get_and_write_data(url, filename)

    if write_schema:
        write_schema()
        write_data()

        while next_url and (('/'.join(url.split('/')[:9]) + '/') != next_url):
            ws, wd, next_url = get_and_write_data(next_url, filename)
            wd()

def parse_response(response):
    to_dict = response.content.decode('utf-8')
    records = json.loads(to_dict)
    
    header = records.pop(0)

    if not isinstance(header, dict):
        return (None,None)
        
    data = (record for record in records)
    return (header, data)

def parse_records(parsed_response):
    """
    Takes a parsed response argument which contains
    the header information and raw data.
    """
    header, data = parsed_response
    
    values = []
    for record in data:
        schema = ','.join(record['_fields'].keys())        
        values.append(
            ','.join([f'{val.strip()}' for val in record['_fields'].values()])
        )

    return (schema, values)

def lock_resource(func):
    def wrapper(*args, **kwargs):
        lock = threading.Lock()

        with lock:
            ret = func(**kwargs)

        return ret

    return wrapper

def write_to_file(data, filename, encoding='utf-8'):
    with open(filename, '+a', encoding=encoding) as f:
        f.write(data)
            
if __name__ == '__main__':
    tenant = 'IDAHO_PRD'
    oauth_request = new_oauth_object(
        config=None,
        tenant_name=tenant
    )

    endpoints = FSMEndpoints(tenant_name='IDAHO_PRD')

    data_extractor = APIExtractor(
        oauth_request=oauth_request, 
        endpoints=endpoints
    )

    bc_name = 'GLTransactionDetail'
    limit = 1
    fields = "APPaid,AccountingEntity,AutoReverse,BaseZoneAccountingUnit,BaseZoneDimension1,Billed,BypassNegativeRateEdit,BypassStructureRelationEdit,BypassUnitAndAmountEdit,Capitalize,ColumnarProjectAmount,ColumnarReplicated,ColumnarSearchAccountingUnit,ColumnarSearchAccountingUnitStructure,ColumnarSearchFinanceDimension1,ColumnarSearchFinanceDimension10,ColumnarSearchFinanceDimension10Structure,ColumnarSearchFinanceDimension1Structure,ColumnarSearchFinanceDimension2,ColumnarSearchFinanceDimension2Structure,ColumnarSearchFinanceDimension3,ColumnarSearchFinanceDimension3Structure,ColumnarSearchFinanceDimension4,ColumnarSearchFinanceDimension4Structure,ColumnarSearchFinanceDimension5,ColumnarSearchFinanceDimension5Structure,ColumnarSearchFinanceDimension6,ColumnarSearchFinanceDimension6Structure,ColumnarSearchFinanceDimension7,ColumnarSearchFinanceDimension7Structure,ColumnarSearchFinanceDimension8,ColumnarSearchFinanceDimension8Structure,ColumnarSearchFinanceDimension9,ColumnarSearchFinanceDimension9Structure,ColumnarSearchProject,ColumnarSearchProjectStructure,ControlDocumentNumber,CreatedByFES,Credit,CurrencyCode,CurrencyTable,Debit,DerivedAPInvoice,DerivedAPInvoiceType,DerivedAPItem,DerivedAPItemDesc,DerivedAPItemType,DerivedAPPayablesInvoice,DerivedAPPurchOrder,DerivedAPQuantity,DerivedAPTaxCode,DerivedAPUnitCost,DerivedAPUom,DerivedAPVendor,DerivedAddOnCharge,DerivedAlternate2CurrencyRate,DerivedAlternate3CurrencyRate,DerivedAlternateAmount,DerivedAlternateAmount2,DerivedAlternateAmount3,DerivedAlternateCurrency,DerivedAlternateCurrencyRate,DerivedBuyer,DerivedCompany,DerivedExcludeInvoice,DerivedFunctionalAmount,DerivedFunctionalCurrency,DerivedFunctionalCurrencyRate,DerivedGLTranEntityYearPeriod,DerivedICDocumentType,DerivedICInventoryTransaction,DerivedICPostingType,DerivedId1,DerivedId2,DerivedId3,DerivedId4,DerivedId5,DerivedId6,DerivedInventoryLocation,DerivedLineAmount,DerivedLineNumber,DerivedOrigICDocument,DerivedOrigICLine,DerivedPOLine,DerivedPOUniqueID,DerivedProjectNum,DerivedPurchaseLoc,DerivedRNIQuantity,DerivedReceivedQuantity,DerivedRequester,DerivedRequestingLocation,DerivedRequisition,DerivedRequisitionLine,DerivedVendorGroupOnly,DerivedVendorOnly,DerivedWorkOrderNum,Description,DimensionCode,DocumentNumber,DrillBackLink,EligibleToTransfer,ExcludeAccount,ExcludeAccountingUnit,ExcludeExpenseInvoices,FESManuallyUpdated,FinanceCodeBlock,FinanceCodeBlock.AccountingUnit,FinanceCodeBlock.FinanceDimension1,FinanceCodeBlock.FinanceDimension10,FinanceCodeBlock.FinanceDimension2,FinanceCodeBlock.FinanceDimension3,FinanceCodeBlock.FinanceDimension4,FinanceCodeBlock.FinanceDimension5,FinanceCodeBlock.FinanceDimension6,FinanceCodeBlock.FinanceDimension7,FinanceCodeBlock.FinanceDimension8,FinanceCodeBlock.FinanceDimension9,FinanceCodeBlock.GeneralLedgerChartAccount,FinanceCodeBlock.Ledger,FinanceCodeBlock.Project,FinanceCodeBlock.ToAccountingEntity,FinanceEnterpriseGroup,FromRNIUpdate,GLTObjectId,GLTransactionDetail,GeneralLedgerEvent,HROrganizationUnit,IndirectBurden,IntercompanyBillingSettlementHeader,IntercompanyBillingSettlementHeader.PayablesBankTransactionCode,IntercompanyBillingSettlementHeader.PayablesCashCode,IntercompanyBillingSettlementHeader.PayablesCompany,IntercompanyBillingSettlementHeader.SettlementID,IsNotValidGeneralLedgerEvent,IsSystemPR,Job,JournalByJournalCode,JournalCode,JournalizeGroup,LaborDistribution,MatchesAccountSearch,MatchesAccountingEntitySearch,MatchesAccountingUnitSearch,MatchesColumnarSearchAccountingUnit,MatchesColumnarSearchFinanceDimension1,MatchesColumnarSearchFinanceDimension10,MatchesColumnarSearchFinanceDimension2,MatchesColumnarSearchFinanceDimension3,MatchesColumnarSearchFinanceDimension4,MatchesColumnarSearchFinanceDimension5,MatchesColumnarSearchFinanceDimension6,MatchesColumnarSearchFinanceDimension7,MatchesColumnarSearchFinanceDimension8,MatchesColumnarSearchFinanceDimension9,MatchesColumnarSearchFinanceDimensions,MatchesColumnarSearchProject,MatchesFinDim10Search,MatchesFinDim1Search,MatchesFinDim2Search,MatchesFinDim3Search,MatchesFinDim4Search,MatchesFinDim5Search,MatchesFinDim6Search,MatchesFinDim7Search,MatchesFinDim8Search,MatchesFinDim9Search,MatchesPostToEntitySearch,MatchesProjectSearch,MatchesSystemCodeSearch,MatchesValidSystems,MigStatus,NotSystemTransactionEvents,OrganizationKeyFields,OriginalLaborTransaction,OriginatingTransaction,OriginatingTransaction.BusinessClassName,OriginatingTransaction.BusinessObjectKey,OriginatingTransactionPeriod,OriginatingTransactionUniqueID,PayCode,Position,PostingDate,PostingDateJournalCodeKey,PostingDateRange,PostingDateRange.BeginDate,PostingDateRange.EndDate,PostingWithinDateRange,PrimaryLedger,ProjectDateRange,ProjectDateRange.BeginDate,ProjectDateRange.EndDate,ProjectHoldCapitalized,ProjectUnProcessedCapitalized,Reference,RelatedJournalTransaction,RelatedJournalTransaction.BusinessClassName,RelatedJournalTransaction.BusinessObjectKey,ReportCurrencyAmount,ReportCurrencyAmount.AlternateAmount,ReportCurrencyAmount.AlternateAmount.EnteredCurrencyAmount,ReportCurrencyAmount.AlternateAmount.EnteredCurrencyRate,ReportCurrencyAmount.AlternateAmount2,ReportCurrencyAmount.AlternateAmount2.EnteredCurrencyAmount,ReportCurrencyAmount.AlternateAmount2.EnteredCurrencyRate,ReportCurrencyAmount.AlternateAmount3,ReportCurrencyAmount.AlternateAmount3.EnteredCurrencyAmount,ReportCurrencyAmount.AlternateAmount3.EnteredCurrencyRate,ReportCurrencyAmount.FunctionalAmount,ReportCurrencyAmount.FunctionalAmount.EnteredCurrencyAmount,ReportCurrencyAmount.FunctionalAmount.EnteredCurrencyRate,ReportCurrencyAmount.ProjectAmount,ReportCurrencyAmount.ProjectAmount.EnteredCurrencyAmount,ReportCurrencyAmount.ProjectAmount.EnteredCurrencyRate,ReportCurrencyAmount.ReportAmount1,ReportCurrencyAmount.ReportAmount1.EnteredCurrencyAmount,ReportCurrencyAmount.ReportAmount1.EnteredCurrencyRate,ReportCurrencyAmount.ReportAmount2,ReportCurrencyAmount.ReportAmount2.EnteredCurrencyAmount,ReportCurrencyAmount.ReportAmount2.EnteredCurrencyRate,ReportCurrencyAmount.ReportAmount3,ReportCurrencyAmount.ReportAmount3.EnteredCurrencyAmount,ReportCurrencyAmount.ReportAmount3.EnteredCurrencyRate,ReportCurrencyAmount.ReportAmount4,ReportCurrencyAmount.ReportAmount4.EnteredCurrencyAmount,ReportCurrencyAmount.ReportAmount4.EnteredCurrencyRate,ReportCurrencyAmount.ReportAmount5,ReportCurrencyAmount.ReportAmount5.EnteredCurrencyAmount,ReportCurrencyAmount.ReportAmount5.EnteredCurrencyRate,ReportCurrencyAmount.ToAlternateAmount,ReportCurrencyAmount.ToAlternateAmount.EnteredCurrencyAmount,ReportCurrencyAmount.ToAlternateAmount.EnteredCurrencyRate,ReportCurrencyAmount.ToAlternateAmount2,ReportCurrencyAmount.ToAlternateAmount2.EnteredCurrencyAmount,ReportCurrencyAmount.ToAlternateAmount2.EnteredCurrencyRate,ReportCurrencyAmount.ToAlternateAmount3,ReportCurrencyAmount.ToAlternateAmount3.EnteredCurrencyAmount,ReportCurrencyAmount.ToAlternateAmount3.EnteredCurrencyRate,ReportCurrencyAmount.ToFunctionalAmount,ReportCurrencyAmount.ToFunctionalAmount.EnteredCurrencyAmount,ReportCurrencyAmount.ToFunctionalAmount.EnteredCurrencyRate,Resource,RevenueRecognized,SearchAccount,SearchAccount.AccountGroup,SearchAccountingEntity,SearchAccountingEntity.AccountingEntityGroup,SearchAccountingUnit,SearchAccountingUnit.AccountingUnitGroup,SearchDateRange,SearchDateRange.Begin,SearchDateRange.End,SearchFinanceDimension1,SearchFinanceDimension1.FinanceDimension1Group,SearchFinanceDimension10,SearchFinanceDimension10.FinanceDimension10Group,SearchFinanceDimension2,SearchFinanceDimension2.FinanceDimension2Group,SearchFinanceDimension3,SearchFinanceDimension3.FinanceDimension3Group,SearchFinanceDimension4,SearchFinanceDimension4.FinanceDimension4Group,SearchFinanceDimension5,SearchFinanceDimension5.FinanceDimension5Group,SearchFinanceDimension6,SearchFinanceDimension6.FinanceDimension6Group,SearchFinanceDimension7,SearchFinanceDimension7.FinanceDimension7Group,SearchFinanceDimension8,SearchFinanceDimension8.FinanceDimension8Group,SearchFinanceDimension9,SearchFinanceDimension9.FinanceDimension9Group,SearchGroup,SearchPostToEntity,SearchPostToEntity.PostToEntityGroup,SearchProject,SearchProject.ProjectGroup,SearchSystemCode,SearchSystemCode.SystemCodeGroup,SearchVendor,SearchVendor.SearchVendorGroup,SecurityGroupAllowsAccess,SecurityGroupAllowsAccessBlankValuesExcluded,SkipFESExpenditures,Status,System,TransactionAmount,TransactionDate,TransientDefaultCurrencyAmount,TransientInvoiceType,TransientSkipAttachRule,TransientVendorGroup,TransientVendorOnly,UnProcessedBilled,UnProcessedCapitalized,UnProcessedIndirectBurden,UnProcessedLaborDistribution,UnitsAmount,UseDrillBackLink,UseOriginatingTransaction,VendorGroupAndVendor,VendorGroupAndVendor.Vendor,VendorGroupAndVendor.VendorGroup,ZoneAccountingUnit,ZoneDimension1,ZoneFields,ZoneFinanceCodeBlock,ZoneFinanceCodeBlock.AccountingUnit,ZoneFinanceCodeBlock.FinanceDimension1,ZoneFinanceCodeBlock.FinanceDimension10,ZoneFinanceCodeBlock.FinanceDimension2,ZoneFinanceCodeBlock.FinanceDimension3,ZoneFinanceCodeBlock.FinanceDimension4,ZoneFinanceCodeBlock.FinanceDimension5,ZoneFinanceCodeBlock.FinanceDimension6,ZoneFinanceCodeBlock.FinanceDimension7,ZoneFinanceCodeBlock.FinanceDimension8,ZoneFinanceCodeBlock.FinanceDimension9,ZoneFinanceCodeBlock.GeneralLedgerChartAccount,ZoneFinanceCodeBlock.Ledger,ZoneFinanceCodeBlock.Project,ZoneFinanceCodeBlock.ToAccountingEntity,ZoneSystemKeyFields"    
    url = '&'.join([endpoints.GENERIC_LIST.format(
        bc_name=bc_name,
        fields=fields,
        limit=limit
    )] + ['&_lplFilter=PostingDate%20%3C%20%222023-10-01%22%20and%20AccountingEntity%20%3D%20%22{agency}%22'])
    # agencies = [
    # 951,
    # 952,
    # 953,
    # 954,
    # 955,
    # 956,
    # 957,
    # 999
    # ]
    with open('agencies.csv', 'r') as f1:
        agencies = [agy.strip() for agy in f1.readlines()]    
    urls = list(map(lambda agency: url.format(agency=agency), agencies))

    # Create and start threads
    threads = []
    for i in range(len(agencies)):
        thread = threading.Thread(target=get_api_header, args=(urls[i],bc_name, agencies[i]))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    print('Done')
