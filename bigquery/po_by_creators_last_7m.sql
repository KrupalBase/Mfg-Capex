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
)
SELECT
  po.id AS po_id,
  po.name AS po_number,
  po.date_order,
  po.date_approve,
  po.state AS po_state,
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
  po.write_date AS po_updated_date
FROM `gtm-analytics-447201.odoo_public.purchase_order` po
JOIN `gtm-analytics-447201.odoo_public.purchase_order_line` pol ON pol.order_id = po.id
LEFT JOIN `gtm-analytics-447201.odoo_public.res_partner` v ON po.partner_id = v.id
LEFT JOIN `gtm-analytics-447201.odoo_public.account_analytic_account` aaa ON pol.analytic_account_project_id = aaa.id
LEFT JOIN `gtm-analytics-447201.odoo_public.res_users` creator_u ON po.create_uid = creator_u.id
LEFT JOIN `gtm-analytics-447201.odoo_public.res_partner` creator_p ON creator_u.partner_id = creator_p.id
WHERE po.create_uid IN (SELECT user_id FROM creators)
  AND po.date_order >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 MONTH)
ORDER BY po.date_order DESC, po.id, pol.sequence;
