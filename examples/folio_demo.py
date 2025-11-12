# This script uses LDLite to extract sample data from the FOLIO demo sites.
from __future__ import annotations

import sys

import httpx

import ldlite

# Demo sites
eureka_snapshot = "https://folio-etesting-snapshot-kong.ci.folio.org"

###############################################################################
# Select a demo site here:
selected_site = eureka_snapshot
###############################################################################
# Note that these demo sites are unavailable at certain times in the evening
# (Eastern time) or if a bug is introduced and makes one of them unresponsive.
# For information about the status of the demo sites, please see the
# hosted-reference-envs channel in the FOLIO Slack organization. For general
# information about FOLIO demo sites, see the "Reference Environments" section
# of the FOLIO Wiki at:
# https://folio-org.atlassian.net/wiki/spaces/FOLIJET/pages/513704182/Reference+environments
###############################################################################

ld = ldlite.LDLite()
ld.connect_folio(
    url=selected_site,
    tenant="diku",
    user="diku_admin",
    password="admin",
)

ld.experimental_connect_db_sqlite(filename="ldlite.sqlite")
# For PostgreSQL, use connect_db_postgresql() instead:
# ld.connect_db_postgresql(dsn='dbname=ldlite host=localhost user=ldlite')

queries: list[tuple[str, ...] | tuple[str, str, object, int]] = [
    ("folio_agreements.entitlement", "/erm/entitlements"),
    ("folio_agreements.erm_resource", "/erm/resource"),
    ("folio_agreements.org", "/erm/org"),
    ("folio_agreements.refdata_value", "/erm/refdata"),
    # This endpoint doesn't work in EBSCO environments
    # ("folio_agreements.usage_data_provider", "/usage-data-providers"),
    ("folio_audit.circulation_logs", "/audit-data/circulation/logs"),
    ("folio_circulation.audit_loan", "/loan-storage/loan-history"),
    (
        "folio_circulation.cancellation_reason",
        "/cancellation-reason-storage/cancellation-reasons",
    ),
    ("folio_circulation.check_in", "/check-in-storage/check-ins"),
    (
        "folio_circulation.fixed_due_date_schedule",
        "/fixed-due-date-schedule-storage/fixed-due-date-schedules",
    ),
    ("folio_circulation.loan", "/loan-storage/loans"),
    ("folio_circulation.loan_policy", "/loan-policy-storage/loan-policies"),
    (
        "folio_circulation.patron_action_session",
        "/patron-action-session-storage/patron-action-sessions",
    ),
    (
        "folio_circulation.patron_notice_policy",
        "/patron-notice-policy-storage/patron-notice-policies",
    ),
    ("folio_circulation.request", "/request-storage/requests"),
    ("folio_circulation.request_policy", "/request-policy-storage/request-policies"),
    (
        "folio_circulation.scheduled_notice",
        "/scheduled-notice-storage/scheduled-notices",
    ),
    ("folio_circulation.staff_slips", "/staff-slips-storage/staff-slips"),
    (
        "folio_circulation.user_request_preference",
        "/request-preference-storage/request-preference",
    ),
    ("folio_configuration.config_data", "/configurations/entries"),
    (
        "folio_courses.coursereserves_copyrightstates",
        "/coursereserves/copyrightstatuses",
    ),
    ("folio_courses.coursereserves_courselistings", "/coursereserves/courselistings"),
    ("folio_courses.coursereserves_courses", "/coursereserves/courses"),
    ("folio_courses.coursereserves_coursetypes", "/coursereserves/coursetypes"),
    ("folio_courses.coursereserves_departments", "/coursereserves/departments"),
    (
        "folio_courses.coursereserves_processingstates",
        "/coursereserves/processingstatuses",
    ),
    ("folio_courses.coursereserves_reserves", "/coursereserves/reserves"),
    ("folio_courses.coursereserves_roles", "/coursereserves/roles"),
    ("folio_courses.coursereserves_terms", "/coursereserves/terms"),
    ("folio_erm_usage.counter_reports", "/counter-reports"),
    ("folio_feesfines.accounts", "/accounts"),
    ("folio_feesfines.comments", "/comments"),
    ("folio_feesfines.feefineactions", "/feefineactions"),
    ("folio_feesfines.feefines", "/feefines"),
    ("folio_feesfines.lost_item_fee_policy", "/lost-item-fees-policies"),
    ("folio_feesfines.manualblocks", "/manualblocks"),
    ("folio_feesfines.overdue_fine_policy", "/overdue-fines-policies"),
    ("folio_feesfines.owners", "/owners"),
    ("folio_feesfines.payments", "/payments"),
    ("folio_feesfines.refunds", "/refunds"),
    ("folio_feesfines.transfer_criteria", "/transfer-criterias"),
    ("folio_feesfines.transfers", "/transfers"),
    ("folio_feesfines.waives", "/waives"),
    ("folio_finance.budget", "/finance-storage/budgets"),
    ("folio_finance.expense_class", "/finance-storage/expense-classes"),
    ("folio_finance.fiscal_year", "/finance-storage/fiscal-years"),
    ("folio_finance.fund", "/finance-storage/funds"),
    ("folio_finance.fund_type", "/finance-storage/fund-types"),
    (
        "folio_finance.group_fund_fiscal_year",
        "/finance-storage/group-fund-fiscal-years",
    ),
    ("folio_finance.groups", "/finance-storage/groups"),
    ("folio_finance.ledger", "/finance-storage/ledgers"),
    ("folio_finance.transaction", "/finance-storage/transactions"),
    ("folio_inventory.alternative_title_type", "/alternative-title-types"),
    ("folio_inventory.call_number_type", "/call-number-types"),
    ("folio_inventory.classification_type", "/classification-types"),
    ("folio_inventory.contributor_name_type", "/contributor-name-types"),
    ("folio_inventory.contributor_type", "/contributor-types"),
    (
        "folio_inventory.electronic_access_relationship",
        "/electronic-access-relationships",
    ),
    ("folio_inventory.holdings_note_type", "/holdings-note-types"),
    ("folio_inventory.holdings_record", "/holdings-storage/holdings"),
    ("folio_inventory.holdings_records_source", "/holdings-sources"),
    ("folio_inventory.holdings_type", "/holdings-types"),
    ("folio_inventory.identifier_type", "/identifier-types"),
    ("folio_inventory.ill_policy", "/ill-policies"),
    ("folio_inventory.instance", "/instance-storage/instances"),
    ("folio_inventory.instance_format", "/instance-formats"),
    ("folio_inventory.instance_note_type", "/instance-note-types"),
    (
        "folio_inventory.instance_relationship",
        "/instance-storage/instance-relationships",
    ),
    ("folio_inventory.instance_relationship_type", "/instance-relationship-types"),
    ("folio_inventory.instance_status", "/instance-statuses"),
    ("folio_inventory.instance_type", "/instance-types"),
    ("folio_inventory.item", "/item-storage/items"),
    ("folio_inventory.item_damaged_status", "/item-damaged-statuses"),
    ("folio_inventory.item_note_type", "/item-note-types"),
    ("folio_inventory.loan_type", "/loan-types"),
    ("folio_inventory.location", "/locations"),
    ("folio_inventory.loccampus", "/location-units/campuses"),
    ("folio_inventory.locinstitution", "/location-units/institutions"),
    ("folio_inventory.loclibrary", "/location-units/libraries"),
    ("folio_inventory.material_type", "/material-types"),
    ("folio_inventory.mode_of_issuance", "/modes-of-issuance"),
    ("folio_inventory.nature_of_content_term", "/nature-of-content-terms"),
    ("folio_inventory.service_point", "/service-points"),
    ("folio_inventory.service_point_user", "/service-points-users"),
    ("folio_inventory.statistical_code", "/statistical-codes"),
    ("folio_inventory.statistical_code_type", "/statistical-code-types"),
    ("folio_invoice.invoice_lines", "/invoice-storage/invoice-lines"),
    ("folio_invoice.invoices", "/invoice-storage/invoices"),
    ("folio_invoice.voucher_lines", "/voucher-storage/voucher-lines"),
    ("folio_invoice.vouchers", "/voucher-storage/vouchers"),
    ("folio_licenses.license", "/licenses/licenses"),
    ("folio_notes.note_data", "/notes"),
    ("folio_orders.acquisitions_unit", "/acquisitions-units-storage/units"),
    (
        "folio_orders.acquisitions_unit_membership",
        "/acquisitions-units-storage/memberships",
    ),
    ("folio_orders.alert", "/orders-storage/alerts"),
    ("folio_orders.order_invoice_relationship", "/orders-storage/order-invoice-relns"),
    ("folio_orders.order_templates", "/orders-storage/order-templates"),
    ("folio_orders.pieces", "/orders-storage/pieces"),
    ("folio_orders.po_line", "/orders-storage/po-lines"),
    ("folio_orders.purchase_order", "/orders-storage/purchase-orders"),
    ("folio_orders.reporting_code", "/orders-storage/reporting-codes"),
    ("folio_organizations.addresses", "/organizations-storage/addresses"),
    ("folio_organizations.categories", "/organizations-storage/categories"),
    ("folio_organizations.contacts", "/organizations-storage/contacts"),
    ("folio_organizations.emails", "/organizations-storage/emails"),
    ("folio_organizations.interfaces", "/organizations-storage/interfaces"),
    ("folio_organizations.organizations", "/organizations/organizations"),
    ("folio_organizations.phone_numbers", "/organizations-storage/phone-numbers"),
    ("folio_organizations.urls", "/organizations-storage/urls"),
    ("folio_source_record.records", "/source-storage/records", 2),
    ("folio_users.addresstype", "/addresstypes"),
    ("folio_users.departments", "/departments"),
    ("folio_users.groups", "/groups"),
    ("folio_users.proxyfor", "/proxiesfor"),
    ("folio_users.users", "/users"),
]

errors: list[tuple[str, BaseException]] = []
tables: list[str] = []
for q in queries:
    try:
        if len(q) == 3:
            tables += ld.query(
                table=q[0],
                path=q[1],
                json_depth=int(q[2]),
            )
        else:
            tables += ld.query(table=q[0], path=q[1])
    except (ValueError, RuntimeError, httpx.HTTPError) as e:
        errors += [(q[1], e)]
print()
print("Tables:")
for t in tables:
    print(t)
print("(" + str(len(tables)) + " tables)")
if len(errors) > 0:
    print()
    print("Errors:")
    for p, e in errors:
        print(
            'folio_demo.py: error processing "' + p + '": ' + str(e),
            file=sys.stderr,
        )
print("(" + str(len(errors)) + " errors)")
