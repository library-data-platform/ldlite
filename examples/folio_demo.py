# This script uses LDLite to extract sample data from the FOLIO demo sites.

import sys
import ldlite

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

ld = ldlite.LDLite()
ld.connect_okapi(url=selected_site, tenant='diku', user='diku_admin', password='admin')

db = ld.connect_db(filename='ldlite.db')
# For PostgreSQL, use connect_db_postgresql() instead of connect_db():
# db = ld.connect_db_postgresql(dsn='dbname=ldlite host=localhost user=ldlite')

allrec = 'cql.allRecords=1 sortby id'

queries = [
    ('folio_agreements.entitlement', '/erm/entitlements', allrec),
    ('folio_agreements.erm_resource', '/erm/resource', allrec),
    ('folio_agreements.org', '/erm/org', allrec),
    ('folio_agreements.refdata_value', '/erm/refdata', allrec),
    ('folio_agreements.usage_data_provider', '/usage-data-providers', allrec),
    ('folio_audit.circulation_logs', '/audit-data/circulation/logs', allrec),
    ('folio_circulation.audit_loan', '/loan-storage/loan-history', allrec),
    ('folio_circulation.cancellation_reason', '/cancellation-reason-storage/cancellation-reasons', allrec),
    ('folio_circulation.check_in', '/check-in-storage/check-ins', allrec),
    ('folio_circulation.fixed_due_date_schedule', '/fixed-due-date-schedule-storage/fixed-due-date-schedules',
     allrec),
    ('folio_circulation.loan', '/loan-storage/loans', allrec),
    ('folio_circulation.loan_policy', '/loan-policy-storage/loan-policies', allrec),
    ('folio_circulation.patron_action_session', '/patron-action-session-storage/patron-action-sessions', allrec),
    ('folio_circulation.patron_notice_policy', '/patron-notice-policy-storage/patron-notice-policies', allrec),
    ('folio_circulation.request', '/request-storage/requests', allrec),
    ('folio_circulation.request_policy', '/request-policy-storage/request-policies', allrec),
    ('folio_circulation.scheduled_notice', '/scheduled-notice-storage/scheduled-notices', allrec),
    ('folio_circulation.staff_slips', '/staff-slips-storage/staff-slips', allrec),
    ('folio_circulation.user_request_preference', '/request-preference-storage/request-preference', allrec),
    ('folio_configuration.config_data', '/configurations/entries', allrec),
    ('folio_courses.coursereserves_copyrightstates', '/coursereserves/copyrightstatuses', allrec),
    ('folio_courses.coursereserves_courselistings', '/coursereserves/courselistings', allrec),
    ('folio_courses.coursereserves_courses', '/coursereserves/courses', allrec),
    ('folio_courses.coursereserves_coursetypes', '/coursereserves/coursetypes', allrec),
    ('folio_courses.coursereserves_departments', '/coursereserves/departments', allrec),
    ('folio_courses.coursereserves_processingstates', '/coursereserves/processingstatuses', allrec),
    ('folio_courses.coursereserves_reserves', '/coursereserves/reserves', allrec),
    ('folio_courses.coursereserves_roles', '/coursereserves/roles', allrec),
    ('folio_courses.coursereserves_terms', '/coursereserves/terms', allrec),
    ('folio_erm_usage.counter_reports', '/counter-reports', allrec),
    ('folio_feesfines.accounts', '/accounts', allrec),
    ('folio_feesfines.comments', '/comments', allrec),
    ('folio_feesfines.feefineactions', '/feefineactions', allrec),
    ('folio_feesfines.feefines', '/feefines', allrec),
    ('folio_feesfines.lost_item_fee_policy', '/lost-item-fees-policies', allrec),
    ('folio_feesfines.manualblocks', '/manualblocks', allrec),
    ('folio_feesfines.overdue_fine_policy', '/overdue-fines-policies', allrec),
    ('folio_feesfines.owners', '/owners', allrec),
    ('folio_feesfines.payments', '/payments', allrec),
    ('folio_feesfines.refunds', '/refunds', allrec),
    ('folio_feesfines.transfer_criteria', '/transfer-criterias', allrec),
    ('folio_feesfines.transfers', '/transfers', allrec),
    ('folio_feesfines.waives', '/waives', allrec),
    ('folio_finance.budget', '/finance-storage/budgets', allrec),
    ('folio_finance.expense_class', '/finance-storage/expense-classes', allrec),
    ('folio_finance.fiscal_year', '/finance-storage/fiscal-years', allrec),
    ('folio_finance.fund', '/finance-storage/funds', allrec),
    ('folio_finance.fund_type', '/finance-storage/fund-types', allrec),
    ('folio_finance.group_fund_fiscal_year', '/finance-storage/group-fund-fiscal-years', allrec),
    ('folio_finance.groups', '/finance-storage/groups', allrec),
    ('folio_finance.ledger', '/finance-storage/ledgers', allrec),
    ('folio_finance.transaction', '/finance-storage/transactions', allrec),
    ('folio_inventory.alternative_title_type', '/alternative-title-types', allrec),
    ('folio_inventory.call_number_type', '/call-number-types', allrec),
    ('folio_inventory.classification_type', '/classification-types', allrec),
    ('folio_inventory.contributor_name_type', '/contributor-name-types', allrec),
    ('folio_inventory.contributor_type', '/contributor-types', allrec),
    ('folio_inventory.electronic_access_relationship', '/electronic-access-relationships', allrec),
    ('folio_inventory.holdings_note_type', '/holdings-note-types', allrec),
    ('folio_inventory.holdings_record', '/holdings-storage/holdings', allrec),
    ('folio_inventory.holdings_records_source', '/holdings-sources', allrec),
    ('folio_inventory.holdings_type', '/holdings-types', allrec),
    ('folio_inventory.identifier_type', '/identifier-types', allrec),
    ('folio_inventory.ill_policy', '/ill-policies', allrec),
    ('folio_inventory.instance', '/instance-storage/instances', allrec),
    ('folio_inventory.instance_format', '/instance-formats', allrec),
    ('folio_inventory.instance_note_type', '/instance-note-types', allrec),
    ('folio_inventory.instance_relationship', '/instance-storage/instance-relationships', allrec),
    ('folio_inventory.instance_relationship_type', '/instance-relationship-types', allrec),
    ('folio_inventory.instance_status', '/instance-statuses', allrec),
    ('folio_inventory.instance_type', '/instance-types', allrec),
    ('folio_inventory.item', '/item-storage/items', allrec),
    ('folio_inventory.item_damaged_status', '/item-damaged-statuses', allrec),
    ('folio_inventory.item_note_type', '/item-note-types', allrec),
    ('folio_inventory.loan_type', '/loan-types', allrec),
    ('folio_inventory.location', '/locations', allrec),
    ('folio_inventory.loccampus', '/location-units/campuses', allrec),
    ('folio_inventory.locinstitution', '/location-units/institutions', allrec),
    ('folio_inventory.loclibrary', '/location-units/libraries', allrec),
    ('folio_inventory.material_type', '/material-types', allrec),
    ('folio_inventory.mode_of_issuance', '/modes-of-issuance', allrec),
    ('folio_inventory.nature_of_content_term', '/nature-of-content-terms', allrec),
    ('folio_inventory.service_point', '/service-points', allrec),
    ('folio_inventory.service_point_user', '/service-points-users', allrec),
    ('folio_inventory.statistical_code', '/statistical-codes', allrec),
    ('folio_inventory.statistical_code_type', '/statistical-code-types', allrec),
    ('folio_invoice.invoice_lines', '/invoice-storage/invoice-lines', allrec),
    ('folio_invoice.invoices', '/invoice-storage/invoices', allrec),
    ('folio_invoice.voucher_lines', '/voucher-storage/voucher-lines', allrec),
    ('folio_invoice.vouchers', '/voucher-storage/vouchers', allrec),
    ('folio_licenses.license', '/licenses/licenses', allrec),
    ('folio_notes.note_data', '/notes', allrec),
    ('folio_orders.acquisitions_unit', '/acquisitions-units-storage/units', allrec),
    ('folio_orders.acquisitions_unit_membership', '/acquisitions-units-storage/memberships', allrec),
    ('folio_orders.alert', '/orders-storage/alerts', allrec),
    ('folio_orders.order_invoice_relationship', '/orders-storage/order-invoice-relns', allrec),
    ('folio_orders.order_templates', '/orders-storage/order-templates', allrec),
    ('folio_orders.pieces', '/orders-storage/pieces', allrec),
    ('folio_orders.po_line', '/orders-storage/po-lines', allrec),
    ('folio_orders.purchase_order', '/orders-storage/purchase-orders', allrec),
    ('folio_orders.reporting_code', '/orders-storage/reporting-codes', allrec),
    ('folio_organizations.addresses', '/organizations-storage/addresses', allrec),
    ('folio_organizations.categories', '/organizations-storage/categories', allrec),
    ('folio_organizations.contacts', '/organizations-storage/contacts', allrec),
    ('folio_organizations.emails', '/organizations-storage/emails', allrec),
    ('folio_organizations.interfaces', '/organizations-storage/interfaces', allrec),
    ('folio_organizations.organizations', '/organizations-storage/organizations', allrec),
    ('folio_organizations.phone_numbers', '/organizations-storage/phone-numbers', allrec),
    ('folio_organizations.urls', '/organizations-storage/urls', allrec),
    ('folio_source_record.records', '/source-storage/records', {}, 2),
    ('folio_users.addresstype', '/addresstypes', allrec),
    ('folio_users.departments', '/departments', allrec),
    ('folio_users.groups', '/groups', allrec),
    ('folio_users.proxyfor', '/proxiesfor', allrec),
    ('folio_users.users', '/users', allrec),
]

tables = []
for q in queries:
    try:
        if len(q) == 4:
            t = ld.query(table=q[0], path=q[1], query=q[2], json_depth=q[3])
        else:
            t = ld.query(table=q[0], path=q[1], query=q[2])
        tables += t
    except (ValueError, RuntimeError):
        print('folio_demo.py: error processing "' + q[1] + '"', file=sys.stderr)
print()
print('Tables:')
for t in tables:
    print(t)
print('(' + str(len(tables)) + ' tables)')
