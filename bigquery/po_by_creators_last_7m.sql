-- POs created by listed team members, last 7 months. Line-level with vendor, project, key fields.
-- Dataset: gtm-analytics-447201.odoo_public

WITH creators AS (
  SELECT u.id AS user_id
  FROM `gtm-analytics-447201.odoo_public.res_users` u
  JOIN `gtm-analytics-447201.odoo_public.res_partner` p ON u.partner_id = p.id
  WHERE LOWER(TRIM(p.name)) IN (
    'alex mitchell', 'ali nik-ahd', 'amber platt', 'andy ross', 'avi anklesaria',
    'benjamin munoz', 'brandon dillard', 'brian connellan', 'callum marsh',
    'chris johnston', 'christopher george', 'christopher vega', 'daleian gopee',
    'diya nair', 'eduardo martinez v.', 'edward pienkowski', 'emerson walter',
    'eric martinez', 'evan pickar', 'ezra doron', 'jamie steele mcdonald',
    'jens emil clausen', 'jimmy kiel', 'juan manrique', 'kelsea allenbaugh',
    'krupal patel', 'kyle morgan', 'kyle wozniak', 'loren grabowski', 'luis gastelum',
    'maintenance bot', 'markia darby', 'mike webb', 'rene santos', 'reyes mata',
    'scott rossi', 'vitor ayres', 'zach patterson', 'zack de la rosa anderson'
  )
),
bill_links AS (
  SELECT
    aml.purchase_line_id AS po_line_id,
    am.id AS bill_id,
    am.payment_state,
    am.state AS bill_state,
    CAST(am.amount_total_signed AS BIGNUMERIC) AS bill_total_signed,
    CAST(am.amount_residual_signed AS BIGNUMERIC) AS bill_residual_signed
  FROM `gtm-analytics-447201.odoo_public.account_move_line` aml
  JOIN `gtm-analytics-447201.odoo_public.account_move` am
    ON am.id = aml.move_id
  WHERE aml.purchase_line_id IS NOT NULL
    AND am.move_type IN ('in_invoice', 'in_refund')
    AND IFNULL(am._fivetran_deleted, FALSE) = FALSE
),
bill_links_dedup AS (
  SELECT * EXCEPT(rn)
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY po_line_id, bill_id ORDER BY bill_id) AS rn
    FROM bill_links
  )
  WHERE rn = 1
),
bill_status_by_line AS (
  SELECT
    po_line_id,
    COUNT(*) AS bill_count,
    SUM(ABS(bill_total_signed)) AS bill_amount_total,
    SUM(GREATEST(ABS(bill_total_signed) - ABS(bill_residual_signed), 0)) AS bill_amount_paid,
    SUM(ABS(bill_residual_signed)) AS bill_amount_open,
    CASE
      WHEN COUNT(*) = 0 THEN 'no_bill'
      WHEN LOGICAL_AND(payment_state = 'paid') THEN 'paid'
      WHEN LOGICAL_OR(payment_state IN ('partial', 'in_payment')) THEN 'partial'
      WHEN LOGICAL_AND(payment_state IN ('not_paid', 'reversed')) THEN 'unpaid'
      ELSE 'mixed'
    END AS bill_payment_status
  FROM bill_links_dedup
  GROUP BY po_line_id
)
SELECT
  po.id AS po_id,
  po.name AS po_number,
  po.date_order,
  po.date_approve,
  po.state AS po_state,
  po.invoice_status AS po_invoice_status,
  po.receipt_status AS po_receipt_status,
  po.partner_id AS vendor_partner_id,
  v.name AS vendor_name,
  v.ref AS vendor_ref,
  v.email AS vendor_email,
  pol.id AS line_id,
  pol.sequence AS line_sequence,
  pol.product_id,
  pol.name AS line_description,
  pol.product_qty,
  pol.qty_received,
  pol.product_uom,
  pol.price_unit,
  pol.price_subtotal,
  pol.price_tax,
  pol.price_total,
  pol.date_planned AS line_date_planned,
  pol.analytic_account_project_id AS project_analytic_id,
  aaa.name AS project_name,
  pol.assigned_project_id,
  po.user_id AS responsible_user_id,
  po.create_uid AS created_by_user_id,
  creator_p.name AS created_by_name,
  po.amount_untaxed AS po_amount_untaxed,
  po.amount_tax AS po_amount_tax,
  po.amount_total AS po_amount_total,
  po.currency_id,
  po.company_id,
  po.origin,
  po.incoterm_id,
  po.dest_address_id,
  po.notes AS po_notes,
  po.create_date AS po_created_date,
  po.write_date AS po_updated_date,
  bsl.bill_count,
  bsl.bill_amount_total,
  bsl.bill_amount_paid,
  bsl.bill_amount_open,
  bsl.bill_payment_status
FROM `gtm-analytics-447201.odoo_public.purchase_order` po
JOIN `gtm-analytics-447201.odoo_public.purchase_order_line` pol ON pol.order_id = po.id
LEFT JOIN `gtm-analytics-447201.odoo_public.res_partner` v ON po.partner_id = v.id
LEFT JOIN `gtm-analytics-447201.odoo_public.account_analytic_account` aaa ON pol.analytic_account_project_id = aaa.id
LEFT JOIN `gtm-analytics-447201.odoo_public.res_users` creator_u ON po.create_uid = creator_u.id
LEFT JOIN `gtm-analytics-447201.odoo_public.res_partner` creator_p ON creator_u.partner_id = creator_p.id
LEFT JOIN bill_status_by_line bsl ON bsl.po_line_id = pol.id
WHERE po.create_uid IN (SELECT user_id FROM creators)
  AND po.date_order >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 MONTH)
ORDER BY po.date_order DESC, po.id, pol.sequence;
