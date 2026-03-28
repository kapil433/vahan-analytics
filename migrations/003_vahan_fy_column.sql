-- Indian financial year label per row (Apr–Mar), aligned with config.mappings.month_to_fy
-- Run after 002_vahan_registrations.sql. Reload cleaned CSV optional (fy column) after this.

ALTER TABLE vahan_registrations ADD COLUMN IF NOT EXISTS fy VARCHAR(12);

UPDATE vahan_registrations SET fy =
  'FY'
  || (CASE WHEN month >= 4 THEN year ELSE year - 1 END)::text
  || '-'
  || RIGHT((CASE WHEN month >= 4 THEN year + 1 ELSE year END)::text, 2)
WHERE fy IS NULL OR fy = '';

CREATE INDEX IF NOT EXISTS idx_vahan_reg_fy ON vahan_registrations(fy);
CREATE INDEX IF NOT EXISTS idx_vahan_reg_state_fy ON vahan_registrations(state_code, fy);
