# This script uses LDLite to extract sample data from the FOLIO demo sites.

# Demo sites
current_release = 'https://folio-juniper-okapi.dev.folio.org/'
latest_snapshot = 'https://folio-snapshot-okapi.dev.folio.org/'

###############################################################################
# Select a demo site here:
selected_site = current_release
###############################################################################
# Note that these demo sites are unavailable at certain times in the evening
# (Eastern time) or if a bug is introduced and makes one of them unresponsive.
# At the time of this writing, the "current release" demo site appears to be
# more stable than the "latest snapshot" site.  For information about the
# status of the demo sites, please see the #hosted-reference-envs channel in
# the FOLIO Slack organization.  For general information about FOLIO demo
# sites, see the "Demo Sites" section of the FOLIO Wiki at:
# https://wiki.folio.org
###############################################################################

import traceback
import ldlite
ld = ldlite.LDLite()
ld.connect_okapi(url=selected_site, tenant='diku', user='diku_admin', password='admin')

db = ld.connect_db(filename='ldlite.db')
# For PostgreSQL, use connect_db_postgresql() instead of connect_db():
# db = ld.connect_db_postgresql(dsn='dbname=ldlite host=localhost user=ldlite')

queries = [
        ('folio_agreements.entitlement', '/erm/entitlements', 'cql.allRecords=1 sortby id'),
        ('folio_agreements.erm_resource', '/erm/resource', 'cql.allRecords=1 sortby id'),
        ('folio_agreements.org', '/erm/org', 'cql.allRecords=1 sortby id'),
        ('folio_agreements.refdata_value', '/erm/refdata', 'cql.allRecords=1 sortby id'),
        ('folio_agreements.usage_data_provider', '/usage-data-providers', 'cql.allRecords=1 sortby id'),
        ('folio_audit.circulation_logs', '/audit-data/circulation/logs', 'cql.allRecords=1 sortby id'),
        ('folio_circulation.audit_loan', '/loan-storage/loan-history', 'cql.allRecords=1 sortby id'),
        ('folio_circulation.cancellation_reason', '/cancellation-reason-storage/cancellation-reasons', 'cql.allRecords=1 sortby id'),
        ('folio_circulation.check_in', '/check-in-storage/check-ins', 'cql.allRecords=1 sortby id'),
        ('folio_circulation.fixed_due_date_schedule', '/fixed-due-date-schedule-storage/fixed-due-date-schedules', 'cql.allRecords=1 sortby id'),
        ('folio_circulation.loan', '/loan-storage/loans', 'cql.allRecords=1 sortby id'),
        ('folio_circulation.loan_policy', '/loan-policy-storage/loan-policies', 'cql.allRecords=1 sortby id'),
        ('folio_circulation.patron_action_session', '/patron-action-session-storage/patron-action-sessions', 'cql.allRecords=1 sortby id'),
        ('folio_circulation.patron_notice_policy', '/patron-notice-policy-storage/patron-notice-policies', 'cql.allRecords=1 sortby id'),
        ('folio_circulation.request', '/request-storage/requests', 'cql.allRecords=1 sortby id'),
        ('folio_circulation.request_policy', '/request-policy-storage/request-policies', 'cql.allRecords=1 sortby id'),
        ('folio_circulation.scheduled_notice', '/scheduled-notice-storage/scheduled-notices', 'cql.allRecords=1 sortby id'),
        ('folio_circulation.staff_slips', '/staff-slips-storage/staff-slips', 'cql.allRecords=1 sortby id'),
        ('folio_circulation.user_request_preference', '/request-preference-storage/request-preference', 'cql.allRecords=1 sortby id'),
        ('folio_configuration.config_data', '/configurations/entries', 'cql.allRecords=1 sortby id'),
        ('folio_courses.coursereserves_copyrightstates', '/coursereserves/copyrightstatuses', 'cql.allRecords=1 sortby id'),
        ('folio_courses.coursereserves_courselistings', '/coursereserves/courselistings', 'cql.allRecords=1 sortby id'),
        ('folio_courses.coursereserves_courses', '/coursereserves/courses', 'cql.allRecords=1 sortby id'),
        ('folio_courses.coursereserves_coursetypes', '/coursereserves/coursetypes', 'cql.allRecords=1 sortby id'),
        ('folio_courses.coursereserves_departments', '/coursereserves/departments', 'cql.allRecords=1 sortby id'),
        ('folio_courses.coursereserves_processingstates', '/coursereserves/processingstatuses', 'cql.allRecords=1 sortby id'),
        ('folio_courses.coursereserves_reserves', '/coursereserves/reserves', 'cql.allRecords=1 sortby id'),
        ('folio_courses.coursereserves_roles', '/coursereserves/roles', 'cql.allRecords=1 sortby id'),
        ('folio_courses.coursereserves_terms', '/coursereserves/terms', 'cql.allRecords=1 sortby id'),
        ('folio_erm_usage.counter_reports', '/counter-reports', 'cql.allRecords=1 sortby id'),
        ('folio_feesfines.accounts', '/accounts', 'cql.allRecords=1 sortby id'),
        ('folio_feesfines.comments', '/comments', 'cql.allRecords=1 sortby id'),
        ('folio_feesfines.feefineactions', '/feefineactions', 'cql.allRecords=1 sortby id'),
        ('folio_feesfines.feefines', '/feefines', 'cql.allRecords=1 sortby id'),
        ('folio_feesfines.lost_item_fee_policy', '/lost-item-fees-policies', 'cql.allRecords=1 sortby id'),
        ('folio_feesfines.manualblocks', '/manualblocks', 'cql.allRecords=1 sortby id'),
        ('folio_feesfines.overdue_fine_policy', '/overdue-fines-policies', 'cql.allRecords=1 sortby id'),
        ('folio_feesfines.owners', '/owners', 'cql.allRecords=1 sortby id'),
        ('folio_feesfines.payments', '/payments', 'cql.allRecords=1 sortby id'),
        ('folio_feesfines.refunds', '/refunds', 'cql.allRecords=1 sortby id'),
        ('folio_feesfines.transfer_criteria', '/transfer-criterias', 'cql.allRecords=1 sortby id'),
        ('folio_feesfines.transfers', '/transfers', 'cql.allRecords=1 sortby id'),
        ('folio_feesfines.waives', '/waives', 'cql.allRecords=1 sortby id'),
        ('folio_finance.budget', '/finance-storage/budgets', 'cql.allRecords=1 sortby id'),
        ('folio_finance.expense_class', '/finance-storage/expense-classes', 'cql.allRecords=1 sortby id'),
        ('folio_finance.fiscal_year', '/finance-storage/fiscal-years', 'cql.allRecords=1 sortby id'),
        ('folio_finance.fund', '/finance-storage/funds', 'cql.allRecords=1 sortby id'),
        ('folio_finance.fund_type', '/finance-storage/fund-types', 'cql.allRecords=1 sortby id'),
        ('folio_finance.group_fund_fiscal_year', '/finance-storage/group-fund-fiscal-years', 'cql.allRecords=1 sortby id'),
        ('folio_finance.groups', '/finance-storage/groups', 'cql.allRecords=1 sortby id'),
        ('folio_finance.ledger', '/finance-storage/ledgers', 'cql.allRecords=1 sortby id'),
        ('folio_finance.transaction', '/finance-storage/transactions', 'cql.allRecords=1 sortby id'),
        ('folio_inventory.alternative_title_type', '/alternative-title-types', 'cql.allRecords=1 sortby id'),
        ('folio_inventory.call_number_type', '/call-number-types', 'cql.allRecords=1 sortby id'),
        ('folio_inventory.classification_type', '/classification-types', 'cql.allRecords=1 sortby id'),
        ('folio_inventory.contributor_name_type', '/contributor-name-types', 'cql.allRecords=1 sortby id'),
        ('folio_inventory.contributor_type', '/contributor-types', 'cql.allRecords=1 sortby id'),
        ('folio_inventory.electronic_access_relationship', '/electronic-access-relationships', 'cql.allRecords=1 sortby id'),
        ('folio_inventory.holdings_note_type', '/holdings-note-types', 'cql.allRecords=1 sortby id'),
        ('folio_inventory.holdings_record', '/holdings-storage/holdings', 'cql.allRecords=1 sortby id'),
        ('folio_inventory.holdings_records_source', '/holdings-sources', 'cql.allRecords=1 sortby id'),
        ('folio_inventory.holdings_type', '/holdings-types', 'cql.allRecords=1 sortby id'),
        ('folio_inventory.identifier_type', '/identifier-types', 'cql.allRecords=1 sortby id'),
        ('folio_inventory.ill_policy', '/ill-policies', 'cql.allRecords=1 sortby id'),
        ('folio_inventory.instance', '/instance-storage/instances', 'cql.allRecords=1 sortby id'),
        ('folio_inventory.instance_format', '/instance-formats', 'cql.allRecords=1 sortby id'),
        ('folio_inventory.instance_note_type', '/instance-note-types', 'cql.allRecords=1 sortby id'),
        ('folio_inventory.instance_relationship', '/instance-storage/instance-relationships', 'cql.allRecords=1 sortby id'),
        ('folio_inventory.instance_relationship_type', '/instance-relationship-types', 'cql.allRecords=1 sortby id'),
        ('folio_inventory.instance_status', '/instance-statuses', 'cql.allRecords=1 sortby id'),
        ('folio_inventory.instance_type', '/instance-types', 'cql.allRecords=1 sortby id'),
        ('folio_inventory.item', '/item-storage/items', 'cql.allRecords=1 sortby id'),
        ('folio_inventory.item_damaged_status', '/item-damaged-statuses', 'cql.allRecords=1 sortby id'),
        ('folio_inventory.item_note_type', '/item-note-types', 'cql.allRecords=1 sortby id'),
        ('folio_inventory.loan_type', '/loan-types', 'cql.allRecords=1 sortby id'),
        ('folio_inventory.location', '/locations', 'cql.allRecords=1 sortby id'),
        ('folio_inventory.loccampus', '/location-units/campuses', 'cql.allRecords=1 sortby id'),
        ('folio_inventory.locinstitution', '/location-units/institutions', 'cql.allRecords=1 sortby id'),
        ('folio_inventory.loclibrary', '/location-units/libraries', 'cql.allRecords=1 sortby id'),
        ('folio_inventory.material_type', '/material-types', 'cql.allRecords=1 sortby id'),
        ('folio_inventory.mode_of_issuance', '/modes-of-issuance', 'cql.allRecords=1 sortby id'),
        ('folio_inventory.nature_of_content_term', '/nature-of-content-terms', 'cql.allRecords=1 sortby id'),
        ('folio_inventory.service_point', '/service-points', 'cql.allRecords=1 sortby id'),
        ('folio_inventory.service_point_user', '/service-points-users', 'cql.allRecords=1 sortby id'),
        ('folio_inventory.statistical_code', '/statistical-codes', 'cql.allRecords=1 sortby id'),
        ('folio_inventory.statistical_code_type', '/statistical-code-types', 'cql.allRecords=1 sortby id'),
        ('folio_invoice.invoice_lines', '/invoice-storage/invoice-lines', 'cql.allRecords=1 sortby id'),
        ('folio_invoice.invoices', '/invoice-storage/invoices', 'cql.allRecords=1 sortby id'),
        ('folio_invoice.voucher_lines', '/voucher-storage/voucher-lines', 'cql.allRecords=1 sortby id'),
        ('folio_invoice.vouchers', '/voucher-storage/vouchers', 'cql.allRecords=1 sortby id'),
        ('folio_licenses.license', '/licenses/licenses', 'cql.allRecords=1 sortby id'),
        ('folio_notes.note_data', '/notes', 'cql.allRecords=1 sortby id'),
        ('folio_orders.acquisitions_unit', '/acquisitions-units-storage/units', 'cql.allRecords=1 sortby id'),
        ('folio_orders.acquisitions_unit_membership', '/acquisitions-units-storage/memberships', 'cql.allRecords=1 sortby id'),
        ('folio_orders.alert', '/orders-storage/alerts', 'cql.allRecords=1 sortby id'),
        ('folio_orders.order_invoice_relationship', '/orders-storage/order-invoice-relns', 'cql.allRecords=1 sortby id'),
        ('folio_orders.order_templates', '/orders-storage/order-templates', 'cql.allRecords=1 sortby id'),
        ('folio_orders.pieces', '/orders-storage/pieces', 'cql.allRecords=1 sortby id'),
        ('folio_orders.po_line', '/orders-storage/po-lines', 'cql.allRecords=1 sortby id'),
        ('folio_orders.purchase_order', '/orders-storage/purchase-orders', 'cql.allRecords=1 sortby id'),
        ('folio_orders.reporting_code', '/orders-storage/reporting-codes', 'cql.allRecords=1 sortby id'),
        ('folio_organizations.addresses', '/organizations-storage/addresses', 'cql.allRecords=1 sortby id'),
        ('folio_organizations.categories', '/organizations-storage/categories', 'cql.allRecords=1 sortby id'),
        ('folio_organizations.contacts', '/organizations-storage/contacts', 'cql.allRecords=1 sortby id'),
        ('folio_organizations.emails', '/organizations-storage/emails', 'cql.allRecords=1 sortby id'),
        ('folio_organizations.interfaces', '/organizations-storage/interfaces', 'cql.allRecords=1 sortby id'),
        ('folio_organizations.organizations', '/organizations-storage/organizations', 'cql.allRecords=1 sortby id'),
        ('folio_organizations.phone_numbers', '/organizations-storage/phone-numbers', 'cql.allRecords=1 sortby id'),
        ('folio_organizations.urls', '/organizations-storage/urls', 'cql.allRecords=1 sortby id'),
        ('folio_users.addresstype', '/addresstypes', 'cql.allRecords=1 sortby id'),
        ('folio_users.departments', '/departments', 'cql.allRecords=1 sortby id'),
        ('folio_users.groups', '/groups', 'cql.allRecords=1 sortby id'),
        ('folio_users.proxyfor', '/proxiesfor', 'cql.allRecords=1 sortby id'),
        ('folio_users.users', '/users', 'cql.allRecords=1 sortby id'),
    ]

tables = []
for q in queries:
    try:
        t = ld.query(table=q[0], path=q[1], query=q[2])
    except Exception as e:
        traceback.print_exception(type(e), e, e.__traceback__)
    tables += t
print()
print('Tables:')
for t in tables:
    print(t)
print('('+str(len(tables))+' tables)')

