-- Run this once in Supabase SQL Editor: https://supabase.com/dashboard/project/wykbukybwzaoddmukfos/sql

CREATE TABLE IF NOT EXISTS corpus_history (
  id          uuid        DEFAULT gen_random_uuid() PRIMARY KEY,
  page        text        NOT NULL,
  metrics     jsonb       NOT NULL DEFAULT '[]'::jsonb,
  feed_items  jsonb       NOT NULL DEFAULT '[]'::jsonb,
  captured_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS corpus_history_page_time
  ON corpus_history (page, captured_at DESC);

ALTER TABLE corpus_history ENABLE ROW LEVEL SECURITY;

CREATE POLICY "anon_read"   ON corpus_history FOR SELECT USING (true);
CREATE POLICY "anon_insert" ON corpus_history FOR INSERT WITH CHECK (true);
