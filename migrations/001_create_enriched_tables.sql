-- Vahan Analytics: Enriched Data Tables
-- Population, Per Capita Income, CNG Stations, EV Chargers
-- Run after state_master exists (or create state_master first)

-- State master (if not exists)
CREATE TABLE IF NOT EXISTS state_master (
  state_code VARCHAR(10) PRIMARY KEY,
  state_name VARCHAR(100) NOT NULL UNIQUE,
  created_at TIMESTAMP DEFAULT NOW()
);

-- Population: state-wise, annual (2011-2036)
CREATE TABLE IF NOT EXISTS state_population (
  id SERIAL PRIMARY KEY,
  state_code VARCHAR(10) NOT NULL REFERENCES state_master(state_code),
  state_name VARCHAR(100) NOT NULL,
  year INT NOT NULL,
  population BIGINT NOT NULL,
  reference_date VARCHAR(20) DEFAULT '1st March',
  source VARCHAR(100) DEFAULT 'MOHFW_Technical_Group_2019',
  fetched_at TIMESTAMP DEFAULT NOW(),
  UNIQUE(state_code, year),
  CONSTRAINT chk_population_positive CHECK (population > 0),
  CONSTRAINT chk_population_year CHECK (year >= 2011 AND year <= 2036)
);

CREATE INDEX idx_state_population_year ON state_population(year);
CREATE INDEX idx_state_population_state ON state_population(state_code);

-- Per Capita Income: state-wise, FY
CREATE TABLE IF NOT EXISTS state_per_capita_income (
  id SERIAL PRIMARY KEY,
  state_code VARCHAR(10) NOT NULL REFERENCES state_master(state_code),
  state_name VARCHAR(100) NOT NULL,
  fy VARCHAR(10) NOT NULL,
  pci_rs DECIMAL(15,2) NOT NULL,
  source VARCHAR(100) DEFAULT 'MOSPI',
  fetched_at TIMESTAMP DEFAULT NOW(),
  UNIQUE(state_code, fy),
  CONSTRAINT chk_pci_positive CHECK (pci_rs > 0),
  CONSTRAINT chk_fy_format CHECK (fy ~ '^\d{4}-\d{2}$')
);

CREATE INDEX idx_state_pci_fy ON state_per_capita_income(fy);
CREATE INDEX idx_state_pci_state ON state_per_capita_income(state_code);

-- CNG Stations: state-wise, monthly
CREATE TABLE IF NOT EXISTS cng_stations (
  id SERIAL PRIMARY KEY,
  state_code VARCHAR(10) NOT NULL REFERENCES state_master(state_code),
  state_name VARCHAR(100) NOT NULL,
  year INT NOT NULL,
  month INT NOT NULL,
  station_count INT NOT NULL,
  source VARCHAR(100) DEFAULT 'PNGRB_CGD_MIS',
  report_date DATE,
  fetched_at TIMESTAMP DEFAULT NOW(),
  UNIQUE(state_code, year, month),
  CONSTRAINT chk_cng_count CHECK (station_count >= 0),
  CONSTRAINT chk_cng_month CHECK (month >= 1 AND month <= 12)
);

CREATE INDEX idx_cng_stations_period ON cng_stations(year, month);
CREATE INDEX idx_cng_stations_state ON cng_stations(state_code);

-- EV Chargers: state-wise, snapshot (year + month, use 12 for annual)
CREATE TABLE IF NOT EXISTS ev_chargers (
  id SERIAL PRIMARY KEY,
  state_code VARCHAR(10) NOT NULL REFERENCES state_master(state_code),
  state_name VARCHAR(100) NOT NULL,
  year INT NOT NULL,
  month INT NOT NULL DEFAULT 12,
  charger_count INT NOT NULL,
  charger_type VARCHAR(20) DEFAULT 'total',
  source VARCHAR(100) DEFAULT 'data.gov.in',
  snapshot_date DATE,
  fetched_at TIMESTAMP DEFAULT NOW(),
  UNIQUE(state_code, year, month),
  CONSTRAINT chk_ev_count CHECK (charger_count >= 0),
  CONSTRAINT chk_ev_month CHECK (month >= 1 AND month <= 12)
);

CREATE INDEX idx_ev_chargers_year ON ev_chargers(year);
CREATE INDEX idx_ev_chargers_state ON ev_chargers(state_code);

-- Seed state_master with Indian states (ISO 3166-2:IN)
INSERT INTO state_master (state_code, state_name) VALUES
  ('AP', 'Andhra Pradesh'),
  ('AR', 'Arunachal Pradesh'),
  ('AS', 'Assam'),
  ('BR', 'Bihar'),
  ('CH', 'Chhattisgarh'),
  ('GA', 'Goa'),
  ('GJ', 'Gujarat'),
  ('HR', 'Haryana'),
  ('HP', 'Himachal Pradesh'),
  ('JK', 'Jammu and Kashmir'),
  ('JH', 'Jharkhand'),
  ('KA', 'Karnataka'),
  ('KL', 'Kerala'),
  ('MP', 'Madhya Pradesh'),
  ('MH', 'Maharashtra'),
  ('MN', 'Manipur'),
  ('ML', 'Meghalaya'),
  ('MZ', 'Mizoram'),
  ('NL', 'Nagaland'),
  ('OR', 'Odisha'),
  ('PB', 'Punjab'),
  ('RJ', 'Rajasthan'),
  ('SK', 'Sikkim'),
  ('TN', 'Tamil Nadu'),
  ('TG', 'Telangana'),
  ('TR', 'Tripura'),
  ('UP', 'Uttar Pradesh'),
  ('UK', 'Uttarakhand'),
  ('WB', 'West Bengal'),
  ('DL', 'Delhi'),
  ('PY', 'Puducherry'),
  ('LD', 'Lakshadweep'),
  ('AN', 'Andaman and Nicobar Islands'),
  ('DN', 'Dadra and Nagar Haveli and Daman and Diu'),
  ('CHD', 'Chandigarh'),
  ('LA', 'Ladakh')
ON CONFLICT (state_code) DO NOTHING;
