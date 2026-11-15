ALTER TABLE local_submitted_data
  ADD COLUMN IF NOT EXISTS province_code TEXT;
