-- Vahan vehicle registration data (Maker, Month Wise)
-- Source: vahan.parivahan.gov.in dashboard
-- Pipeline: scrape -> clean -> load

CREATE TABLE IF NOT EXISTS vahan_registrations (
  id SERIAL PRIMARY KEY,
  state_code VARCHAR(10) NOT NULL,
  state_name VARCHAR(100) NOT NULL,
  year INT NOT NULL,
  fuel_type VARCHAR(30) NOT NULL,
  maker VARCHAR(200) NOT NULL,
  month INT NOT NULL,
  count INT NOT NULL DEFAULT 0,
  source VARCHAR(50) DEFAULT 'vahan_parivahan',
  loaded_at TIMESTAMP DEFAULT NOW(),
  UNIQUE(state_code, state_name, year, fuel_type, maker, month),
  CONSTRAINT chk_vahan_year CHECK (year >= 2012 AND year <= 2030),
  CONSTRAINT chk_vahan_month CHECK (month >= 1 AND month <= 12),
  CONSTRAINT chk_vahan_count CHECK (count >= 0)
);

CREATE INDEX idx_vahan_reg_state_year ON vahan_registrations(state_code, year);
CREATE INDEX idx_vahan_reg_year_fuel ON vahan_registrations(year, fuel_type);
CREATE INDEX idx_vahan_reg_maker ON vahan_registrations(maker, year);

-- Add ALL for All Vahan4 Running States if not in state_master
INSERT INTO state_master (state_code, state_name) VALUES ('ALL', 'All Vahan4 Running States (36/36)')
ON CONFLICT (state_code) DO NOTHING;
